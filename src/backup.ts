import { Clock, Context, Effect, FileSystem, Layer, Option, PlatformError, Redacted, Schema, Stream } from "effect";
import { Hex } from "effect/encoding";
import { HttpClient, HttpClientRequest, type HttpClientResponse } from "effect/http";
import * as SqlClient from "effect/sql/SqlClient";
import { isSqlError } from "effect/sql/SqlError";
import { BackupError, BrokerConfig, LiveSession, ObjectBackupConfig, RunnerConfig, RunnerRow, Sha, SnapshotKey, Task, Work } from "./contracts.ts";
import { Platform } from "./platform.ts";
import { ChildProcess } from "effect/process";
import { Sqlite } from "./sqlite.ts";
import { BrokerPostgres, readDatabaseUrl } from "./postgres.ts";
import { brokerSchemaIdentity } from "./database.ts";
import { sql as expression } from "kysely";
import { makeDatabase } from "./kysely.ts";
import * as Model from "./generated/database/types.ts";

const utf8 = new TextEncoder();
const encode = (value: string) => encodeURIComponent(value).replace(/[!'()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
const failure = () => new BackupError({ message: "Snapshot object storage operation failed" });
const checked = <A>(f: () => A) => Effect.try({ try: f, catch: failure });

const SnapshotList = Schema.Struct({ keys: Schema.Array(SnapshotKey), next: Schema.optional(Schema.String.check(Schema.isMinLength(1))) });
const xmlCharacters = /^[\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\u{10000}-\u{10FFFF}]*$/u;
const decodeXmlText = (text: string): string => {
  if (!xmlCharacters.test(text) || text.includes("]]>")) throw failure();
  return text.replace(/&(?:amp|lt|gt|quot|apos|#[0-9]+|#x[0-9a-fA-F]+);|&/g, (reference) => {
    switch (reference) {
      case "&amp;": return "&";
      case "&lt;": return "<";
      case "&gt;": return ">";
      case "&quot;": return '"';
      case "&apos;": return "'";
      case "&": throw failure();
      default: {
        const number = reference.startsWith("&#x") ? Number.parseInt(reference.slice(3, -1), 16) : Number(reference.slice(2, -1));
        if (!Number.isSafeInteger(number) || number < 0 || number > 0x10ffff) throw failure();
        const character = String.fromCodePoint(number);
        if (!xmlCharacters.test(character)) throw failure();
        return character;
      }
    }
  });
};

export function decodeSnapshotList(text: string): typeof SnapshotList.Type {
  // ponytail: validate S3's small listing envelope, not a general XML document; DTDs and external entities are never accepted.
  if (text.includes("<!") || !/^\s*(?:<\?xml\s[^<>]*\?>\s*)?<ListBucketResult(?:\s[^>]*)?>[\s\S]*<\/ListBucketResult>\s*$/.test(text)) throw failure();
  const tags: string[] = [];
  const fields = new Set<string>();
  let rootSeen = false;
  let through = 0;
  for (const token of text.matchAll(/<[^<>]*>/g)) {
    const value = text.slice(through, token.index);
    if (value.includes("<")) throw failure();
    decodeXmlText(value);
    through = token.index + token[0].length;
    if (/^<\?xml\s+version=(?:"1\.0"|'1\.0')(?:\s+encoding=(?:"UTF-8"|'UTF-8'))?(?:\s+standalone=(?:"(?:yes|no)"|'(?:yes|no)'))?\s*\?>$/i.test(token[0]) && tags.length === 0 && !rootSeen) continue;
    const name = /^<\/?([A-Za-z_][\w.-]*)(?:\s+[A-Za-z_][\w.:-]*\s*=\s*(?:"[^"<]*"|'[^'<]*'))*\s*\/?>$/.exec(token[0])?.[1];
    if (!name) throw failure();
    const attributes = new Set<string>();
    for (const attribute of token[0].matchAll(/\s+([A-Za-z_][\w.:-]*)\s*=\s*("[^"]*"|'[^']*')/g)) {
      const [, attributeName, quotedValue] = attribute;
      if (attributeName === undefined || quotedValue === undefined || attributes.has(attributeName)) throw failure();
      attributes.add(attributeName);
      decodeXmlText(quotedValue.slice(1, -1));
    }
    if (token[0].startsWith("</")) {
      if (token[0] !== `</${name}>` || tags.pop() !== name) throw failure();
    } else {
      if (["Key", "IsTruncated", "NextContinuationToken", "KeyCount", "MaxKeys"].includes(tags.at(-1) ?? "")) throw failure();
      if (["Contents", "Key", "IsTruncated", "NextContinuationToken", "KeyCount", "MaxKeys"].includes(name)) {
        if (token[0] !== `<${name}>` || tags.join("/") !== (name === "Key" ? "ListBucketResult/Contents" : "ListBucketResult")) throw failure();
        if (name !== "Contents" && name !== "Key") {
          if (fields.has(name)) throw failure();
          fields.add(name);
        }
      }
      if (name === "ListBucketResult") {
        if (rootSeen || tags.length !== 0) throw failure();
        rootSeen = true;
      }
      if (!token[0].endsWith("/>")) tags.push(name);
    }
  }
  if (tags.length || text.slice(through).includes("<")) throw failure();
  decodeXmlText(text.slice(through));
  const contents = [...text.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)];
  const keys = contents.map((item) => {
    const matches = [...item[1]!.matchAll(/<Key>([^<]*)<\/Key>/g)];
    if (matches.length !== 1) throw failure();
    return decodeXmlText(matches[0]![1]!);
  });
  const truncated = [...text.matchAll(/<IsTruncated>(true|false)<\/IsTruncated>/g)];
  const next = [...text.matchAll(/<NextContinuationToken>([^<]+)<\/NextContinuationToken>/g)];
  const count = [...text.matchAll(/<KeyCount>([0-9]+)<\/KeyCount>/g)], maximum = [...text.matchAll(/<MaxKeys>([0-9]+)<\/MaxKeys>/g)];
  if ((text.match(/<\/?Key\b/g)?.length ?? 0) !== contents.length * 2 || (text.match(/<\/?Contents\b/g)?.length ?? 0) !== contents.length * 2 || keys.length > 256 || truncated.length !== 1 || next.length !== (truncated[0]![1] === "true" ? 1 : 0) || count.length !== 1 || Number(count[0]![1]) !== keys.length || maximum.length !== 1 || !Number.isSafeInteger(Number(maximum[0]![1])) || Number(maximum[0]![1]) < keys.length || Number(maximum[0]![1]) > 256) throw failure();
  return Schema.decodeUnknownSync(SnapshotList, { onExcessProperty: "error" })({ keys, ...(next.length ? { next: decodeXmlText(next[0]![1]!) } : {}) });
}

const contentLength = (response: HttpClientResponse.HttpClientResponse) => checked(() => {
  const value = response.headers["content-length"];
  if (value === undefined) return undefined;
  if (!/^[0-9]+$/.test(value)) throw failure();
  return BigInt(value);
});

const makeSnapshotStore = Effect.fnUntraced(function*(input: ObjectBackupConfig) {
  const platform = yield* Platform;
  const client = HttpClient.withScope(yield* HttpClient.HttpClient);
  const config = yield* checked(() => Schema.decodeUnknownSync(ObjectBackupConfig, { onExcessProperty: "error" })(input));
  const endpoint = yield* checked(() => {
    const url = new URL(config.endpoint);
    if (!/^https?:\/\/[^/?#@]+\/?$/i.test(config.endpoint)) throw failure();
    if (url.username || url.password || url.search || url.hash || url.pathname !== "/" ||
        (url.protocol !== "https:" && !(url.protocol === "http:" && ["127.0.0.1", "[::1]", "localhost"].includes(url.hostname)))) throw failure();
    return url;
  });
  const accessKey = yield* platform.privateFile(config.accessKeyFile).pipe(Effect.mapError(failure));
  const secretKey = yield* platform.privateFile(config.secretKeyFile).pipe(Effect.mapError(failure));
  if (!/^[A-Za-z0-9]{1,128}$/.test(accessKey) || !secretKey || secretKey.length > 4096 || /\s/.test(secretKey)) return yield* failure();

  let cachedDate = "";
  let cachedSigningKey: Uint8Array | undefined;
  const signed = Effect.fnUntraced(function*(method: "GET" | "PUT" | "DELETE", key?: SnapshotKey, params: ReadonlyArray<readonly [string, string]> = []) {
    const timestamp = new Date(yield* Clock.currentTimeMillis).toISOString().replace(/[:-]|\.\d{3}/g, "");
    const date = timestamp.slice(0, 8);
    const uri = `/${encode(config.bucket)}${key === undefined ? "" : `/${key.split("/").map(encode).join("/")}`}`;
    const query = params.map(([name, value]) => `${encode(name)}=${encode(value)}`).sort().join("&");
    const scope = `${date}/${config.region}/s3/aws4_request`;
    const payload = "UNSIGNED-PAYLOAD";
    const headers = `host:${endpoint.host}\nx-amz-content-sha256:${payload}\nx-amz-date:${timestamp}\n`;
    const names = "host;x-amz-content-sha256;x-amz-date";
    const canonical = `${method}\n${uri}\n${query}\n${headers}\n${names}\n${payload}`;
    const stringToSign = `AWS4-HMAC-SHA256\n${timestamp}\n${scope}\n${yield* platform.digest(canonical)}`;
    let signingKey = cachedSigningKey;
    if (cachedDate !== date || signingKey === undefined) {
      const dateKey = yield* platform.hmac(utf8.encode(`AWS4${secretKey}`), date);
      const regionKey = yield* platform.hmac(dateKey, config.region);
      const serviceKey = yield* platform.hmac(regionKey, "s3");
      signingKey = yield* platform.hmac(serviceKey, "aws4_request");
      cachedSigningKey = signingKey;
      cachedDate = date;
    }
    const signature = Hex.encode(yield* platform.hmac(signingKey, stringToSign));
    return HttpClientRequest.make(method)(`${endpoint.origin}${uri}${query ? `?${query}` : ""}`).pipe(HttpClientRequest.setHeaders({
      host: endpoint.host,
      "x-amz-content-sha256": payload,
      "x-amz-date": timestamp,
      authorization: `AWS4-HMAC-SHA256 Credential=${accessKey}/${scope}, SignedHeaders=${names}, Signature=${signature}`,
    }));
  });

  const responseText = Effect.fnUntraced(function*(response: HttpClientResponse.HttpClientResponse) {
    const expected = yield* contentLength(response);
    const limit = 256 * 1024;
    if (expected !== undefined && expected > BigInt(limit)) return yield* failure();
    let size = 0;
    let text = "";
    const decoder = new TextDecoder("utf-8", { fatal: true });
    yield* Stream.runForEach(response.stream, (chunk) => checked(() => {
      size += chunk.byteLength;
      if (size > limit) throw failure();
      text += decoder.decode(chunk, { stream: true });
    }));
    text += yield* checked(() => decoder.decode());
    if (expected !== undefined && expected !== BigInt(size)) return yield* failure();
    return text;
  });

  const put = Effect.fnUntraced(function*(file: string) {
    const key = yield* checked(() => Schema.decodeUnknownSync(SnapshotKey, { onExcessProperty: "error" })(`sanitized/${platform.path.basename(file)}`));
    const request = yield* signed("PUT", key);
    // ponytail: native custom-format dumps stay on disk and stream directly to object storage.
    const body = yield* HttpClientRequest.bodyFile(request, file, { contentType: "application/vnd.postgresql.pgdump" }).pipe(Effect.provideService(FileSystem.FileSystem, platform.fs));
    const response = yield* client.execute(body);
    if (response.status !== 200) return yield* failure();
    return key;
  }, (effect) => Effect.scoped(effect).pipe(Effect.mapError(failure)));

  const restore = Effect.fnUntraced(function*(inputKey: SnapshotKey, destination: string) {
    const key = yield* checked(() => Schema.decodeUnknownSync(SnapshotKey, { onExcessProperty: "error" })(inputKey));
    const response = yield* client.execute(yield* signed("GET", key));
    if (response.status !== 200) return yield* failure();
    const expected = yield* contentLength(response);
    let complete = false;
    const output = yield* Effect.acquireRelease(
      platform.fs.open(destination, { flag: "wx", mode: 0o600 }),
      () => complete ? Effect.void : platform.fs.remove(destination).pipe(Effect.mapError(failure), Effect.orDie),
    );
    let size = 0n;
    yield* Stream.runForEach(response.stream, (chunk) => Effect.gen(function*() {
      size += BigInt(chunk.byteLength);
      if (expected !== undefined && size > expected) return yield* failure();
      yield* output.writeAll(chunk);
    }));
    if (expected !== undefined && size !== expected) return yield* failure();
    yield* output.sync;
    complete = true;
  }, (effect) => Effect.scoped(effect).pipe(Effect.mapError(failure)));

  const expire = Effect.fnUntraced(function*(cutoff: number) {
    if (!Number.isFinite(cutoff)) return yield* failure();
    const keys = new Set<SnapshotKey>();
    const tokens = new Set<string>();
    let next: string | undefined;
    // Validate every page before deleting anything: a malformed later page cannot trigger partial expiry.
    do {
      const params: Array<readonly [string, string]> = [["list-type", "2"], ["max-keys", "256"], ["prefix", "sanitized/"]];
      if (next !== undefined) params.push(["continuation-token", next]);
      const response = yield* client.execute(yield* signed("GET", undefined, params));
      if (response.status !== 200) return yield* failure();
      const text = yield* responseText(response);
      const page = yield* checked(() => decodeSnapshotList(text));
      if (page.keys.length > 256) return yield* failure();
      for (const key of page.keys) keys.add(key);
      next = page.next;
      if (next !== undefined) {
        if (tokens.has(next)) return yield* failure();
        tokens.add(next);
      }
    } while (next !== undefined);
    let removed = 0;
    for (const key of keys) {
      if (Number(key.slice("sanitized/".length, -".pgdump".length)) >= cutoff) continue;
      const response = yield* client.execute(yield* signed("DELETE", key));
      if (response.status !== 204 && response.status !== 200) return yield* failure();
      removed++;
    }
    return removed;
  }, (effect) => Effect.scoped(effect).pipe(Effect.mapError(failure)));

  return { put, restore, expire };
});

export class SnapshotStore extends Context.Service<SnapshotStore>()("coordinator/SnapshotStore", { make: makeSnapshotStore }) {
  static layer(config: ObjectBackupConfig) { return Layer.effect(this, this.make(config)); }
}

// Private recovery is deliberately separate from sanitized support snapshots.
const RecoveryFile = Schema.Struct({ sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)), schema: Schema.String, rows: Schema.Number, pendingAccepted: Schema.Number });
const RecoveryEntry = Schema.Struct({
  path: Schema.String, type: Schema.Literals(["directory", "file", "symlink"]),
  uid: Schema.Int, gid: Schema.Int, mode: Schema.Int,
  blob: Schema.optional(Schema.String.check(Schema.isPattern(/^[0-9]+$/))), sha256: Schema.optional(Schema.String),
  target: Schema.optional(Schema.String),
});
const RecoveryStat = Schema.Struct({
  mode: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 0xffff })),
  uid: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)), gid: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  nlink: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export const RecoveryMetadata = Schema.Struct({
  kind: Schema.Literal("coordinator-private-recovery"), version: Schema.Literal(2),
  sourceSha: Sha,
  createdAt: Schema.String, broker: RecoveryFile,
  runner: Schema.optional(Schema.Struct({ ...RecoveryFile.fields, id: RunnerConfig.fields.id, layout: Schema.String, entries: Schema.Array(RecoveryEntry) })),
  excluded: Schema.Array(Schema.String),
});
export type RecoveryMetadata = typeof RecoveryMetadata.Type;
const recoveryError = (message: string) => new BackupError({ message });
// Report only typed categories and native operation names, never paths, SQL, causes or subprocess output.
const io = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.mapError((error) => {
  if (error instanceof BackupError) return error;
  const reason = PlatformError.isPlatformError(error)
    ? `platform/${error.reason._tag}/${/^[A-Za-z]+$/.test(error.reason.method) ? error.reason.method : "operation"}`
    : isSqlError(error) ? `sql/${error.reason._tag}` : Schema.isSchemaError(error) ? "schema/decode" : "unclassified";
  return recoveryError(`Recovery operation failed (${reason}); inspect private paths and partial output offline`);
}));

const checkedRecoveryPath = (file: string, type: "file" | "directory", absent = false, traversableRunnerParent = false) => Effect.gen(function*() {
  const { fs, path: paths } = yield* Platform;
  const uid = process.getuid?.();
  if (uid === undefined) return yield* recoveryError("Operator UID unavailable");
  if (!paths.isAbsolute(file) || paths.resolve(file) !== file) return yield* recoveryError("Canonical absolute recovery path required");
  const parent = paths.dirname(file);
  for (let current = parent; ; current = paths.dirname(current)) {
    const info = yield* fs.stat(current);
    if (info.type !== "Directory" || (yield* fs.realPath(current)) !== current ||
      ((info.mode & 0o022) !== 0 && !(Option.isSome(info.uid) && info.uid.value === 0 && (info.mode & 0o1000) !== 0))) return yield* recoveryError("Untrusted recovery path ancestor");
    if (current === paths.dirname(current)) break;
  }
  const owner = yield* fs.stat(parent);
  const parentMode = owner.mode & 0o7777;
  if (Option.isNone(owner.uid) || owner.uid.value !== uid ||
    (traversableRunnerParent ? parentMode !== 0o700 && parentMode !== 0o711 : (parentMode & 0o077) !== 0)) {
    return yield* recoveryError(traversableRunnerParent ? "Runner state parent must be operator-owned mode 0700 or 0711" : "Recovery parent must be operator-owned mode 0700");
  }
  if (absent) {
    if (yield* fs.exists(file)) return yield* recoveryError("Recovery path exists, is insecure, or has unexpected type");
    return yield* fs.readLink(file).pipe(Effect.matchEffect({
      onSuccess: () => Effect.fail(recoveryError("Recovery path exists, is insecure, or has unexpected type")),
      onFailure: (error) => error.reason._tag === "NotFound" ? Effect.void : Effect.fail(error),
    }));
  }
  const info = yield* fs.stat(file);
  if ((yield* fs.realPath(file)) !== file || Option.isNone(info.uid) || info.uid.value !== uid || (info.mode & 0o077) !== 0 ||
    (traversableRunnerParent && (info.mode & 0o7777) !== 0o700) ||
    (type === "file" ? info.type !== "File" || Option.isNone(info.nlink) || info.nlink.value !== 1 : info.type !== "Directory")) return yield* recoveryError("Recovery path exists, is insecure, or has unexpected type");
}).pipe(io);
export const privateRecoveryPath = (file: string, type: "file" | "directory", absent = false) =>
  checkedRecoveryPath(file, type, absent);
// ponytail: Effect Crypto has no incremental digest; its scoped process service keeps hashing bounded.
const hashFile = (file: string) => Effect.gen(function*() {
  const platform = yield* Platform;
  const output = yield* platform.exec(ChildProcess.make("/usr/bin/sha256sum", [], { stdin: platform.fs.stream(file), env: {}, extendEnv: false }), Infinity);
  if (!/^[a-f0-9]{64}  -\n$/.test(output)) return yield* recoveryError("Invalid incremental recovery digest");
  return output.slice(0, 64);
}).pipe(io);
const noSidecars = (file: string) => Effect.gen(function*() {
  for (const suffix of ["-wal", "-shm", "-journal"]) {
    yield* privateRecoveryPath(`${file}${suffix}`, "file", true);
  }
});
const syncPath = (file: string) => Effect.scoped(Effect.gen(function*() {
  const { fs } = yield* Platform;
  yield* (yield* fs.open(file)).sync;
})).pipe(io);

const brokerTables = ["tasks", "sessions", "peers", "consents", "participant_consents", "archive", "evidence", "effect_queue"] as const;
// Only attested application tables are selected; Prisma history, queue migrations and recovery_gate never enter an archive.
const nativePostgres = (config: BrokerConfig, executable: "/usr/bin/pg_dump" | "/usr/bin/pg_restore", args: ReadonlyArray<string>) => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const url = new URL(Redacted.value(yield* readDatabaseUrl(config.databaseUrlFile)));
  const directory = yield* platform.fs.makeTempDirectoryScoped({ prefix: "coordinator-pgpass-" });
  yield* platform.fs.chmod(directory, 0o700);
  const passwordFile = platform.path.join(directory, "pgpass");
  const escape = (value: string) => value.replace(/\\/g, "\\\\").replace(/:/g, "\\:");
  const host = url.hostname.replace(/^\[|\]$/g, "");
  const database = decodeURIComponent(url.pathname.slice(1)), user = decodeURIComponent(url.username);
  yield* platform.fs.writeFileString(passwordFile, [host, url.port || "5432", database, user, decodeURIComponent(url.password)].map(escape).join(":") + "\n", { flag: "wx", mode: 0o600 });
  const version = yield* platform.exec(ChildProcess.make(executable, ["--version"], { env: {}, extendEnv: false })).pipe(io,
    Effect.mapError((error) => recoveryError(`Native PostgreSQL version probe: ${error.message}`)));
  if (!/ \(PostgreSQL\) 18(?:\.|\s)/.test(version)) return yield* recoveryError("PostgreSQL 18 native recovery tools required");
  const ca = process.env.NODE_EXTRA_CA_CERTS;
  // An empty native connection string selects the validated PG* environment without interpreting a database name as conninfo.
  return yield* platform.exec(ChildProcess.make(executable, ["--no-password", ...(executable === "/usr/bin/pg_restore" && !args.includes("--list") ? ["--dbname", ""] : []), ...args], { env: {
    PGHOST: host, PGPORT: url.port || "5432", PGDATABASE: database, PGUSER: user, PGPASSFILE: passwordFile,
    PGSSLMODE: url.searchParams.get("sslmode") ?? "verify-full", ...(ca ? { PGSSLROOTCERT: ca } : {}),
  }, extendEnv: false, stdin: "ignore" }), Infinity).pipe(io,
    Effect.mapError((error) => recoveryError(`Native ${executable === "/usr/bin/pg_dump" ? "pg_dump" : args.includes("--list") ? "pg_restore list" : "pg_restore"} execution: ${error.message}`)));
})).pipe(io);
const validateBrokerDump = (config: BrokerConfig, source: string, full: boolean) => Effect.gen(function*() {
  const listing = yield* nativePostgres(config, "/usr/bin/pg_restore", ["--list", source]);
  const tables = new Set<string>(), sequences = new Set<string>();
  for (const line of listing.split("\n").filter((line) => line && !line.startsWith(";"))) {
    const match = /^\d+; \d+ \d+ (TABLE DATA|SEQUENCE SET) public (\S+) .+$/.exec(line);
    if (!match) return yield* recoveryError("Recovery dump contains unexpected executable/schema entries");
    const name = match[2]!;
    if (match[1] === "TABLE DATA") {
      if (!brokerTables.some((table) => table === name) || tables.has(name)) return yield* recoveryError("Recovery dump contains unexpected/duplicate table data");
      tables.add(name);
    } else {
      if (!["archive_sequence_seq", "evidence_id_seq", "effect_queue_sequence_seq"].includes(name) || sequences.has(name)) return yield* recoveryError("Recovery dump contains unexpected/duplicate sequences");
      sequences.add(name);
    }
  }
  if (brokerTables.some((table) => (full || !["archive", "consents", "participant_consents"].includes(table)) && !tables.has(table)) || sequences.size !== 3) return yield* recoveryError("Recovery dump is missing required data/sequence entries");
});

export const dumpBrokerDatabase = (config: BrokerConfig, destination: string, sanitized: boolean) => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  yield* privateRecoveryPath(destination, "file", true);
  // O_EXCL reserves the destination; native output writes through the already private path.
  yield* platform.fs.writeFileString(destination, "", { flag: "wx", mode: 0o600 });
  let complete = false;
  yield* Effect.addFinalizer(() => complete ? Effect.void : platform.fs.remove(destination).pipe(Effect.orDie));
  const tables = brokerTables.filter((table) => !sanitized || !["archive", "consents", "participant_consents"].includes(table));
  yield* nativePostgres(config, "/usr/bin/pg_dump", ["--format=custom", "--data-only", "--no-owner", "--no-privileges", "--file", destination,
    "--exclude-schema=prisma_contract", ...tables.map((table) => `--table=public.${table}`), "--table=public.archive_sequence_seq", "--table=public.evidence_id_seq", "--table=public.effect_queue_sequence_seq"]);
  yield* syncPath(destination);
  yield* syncPath(platform.path.dirname(destination));
  complete = true;
})).pipe(io);

const inspectBrokerDatabase = (runner?: RunnerConfig) => Effect.gen(function*() {
  const { sql } = yield* BrokerPostgres;
  const schema = yield* brokerSchemaIdentity(sql);
  const db = makeDatabase(sql);
  let after: Model.TasksId | null = null, rows = 0, pendingAccepted = 0;
  for (;;) {
    const page: ReadonlyArray<Model.Tasks> = yield* db.execute(db.client.selectFrom("tasks").selectAll().$if(after !== null, (query) => query.where("id", ">", after!)).orderBy("id").limit(128));
    if (!page.length) break;
    for (const row of page) {
      const task = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Task), { onExcessProperty: "error" })(row.data);
      if (task.taskId !== row.id) return yield* recoveryError("Broker task identifier mismatch");
      if (task.state === "ACCEPTED" || task.state === "RUNNING") pendingAccepted++;
      if (task.runnerId && !["SUCCEEDED", "FAILED"].includes(task.state) && task.runnerId !== runner?.id) return yield* recoveryError("Every nonterminal assigned runner task requires its paired runner recovery config");
      after = row.id; rows++;
    }
  }
  return { schema, rows, pendingAccepted };
});
const validateBrokerRunnerPair = (runner: RunnerConfig, runnerDatabase: string) => Effect.scoped(Effect.gen(function*() {
  const { sql } = yield* BrokerPostgres;
  const { sql: journal } = yield* Sqlite;
  const db = makeDatabase(sql);
  let after: Model.TasksId | null = null;
  for (;;) {
    const page: ReadonlyArray<Model.Tasks> = yield* db.execute(db.client.selectFrom("tasks").selectAll().$if(after !== null, (query) => query.where("id", ">", after!)).orderBy("id").limit(128));
    if (!page.length) break;
    for (const row of page) {
      const task = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Task), { onExcessProperty: "error" })(row.data);
      if (task.runnerId === runner.id && !["SUCCEEDED", "FAILED"].includes(task.state)) {
        const paired = yield* journal.unsafe<{ fingerprint: string }>("SELECT fingerprint FROM tasks WHERE taskId=?", [task.taskId]);
        if (paired.length !== 1 || paired[0]?.fingerprint !== task.fingerprint) return yield* recoveryError("Broker/runner private recovery task pairing mismatch");
      }
      after = row.id;
    }
  }
}).pipe(Effect.provide(Sqlite.layer({ filename: runnerDatabase, readonly: true }))));
const emptyBrokerDatabase = Effect.gen(function*() {
  const { sql } = yield* BrokerPostgres;
  yield* brokerSchemaIdentity(sql);
  const db = makeDatabase(sql);
  for (const table of brokerTables) {
    if ((yield* db.execute(db.client.selectFrom(table).selectAll().limit(1))).length) return yield* recoveryError("Fresh empty migrated PostgreSQL target required");
  }
  const gate = yield* db.execute(db.client.selectFrom("recovery_gate").selectAll().where("id", "=", Model.RecoveryGateId.make(1)));
  if (gate.length) return yield* recoveryError("Recovery target has already been used or fenced; provision a fresh database");
});
const fenceBroker = Effect.gen(function*() {
  const { sql } = yield* BrokerPostgres;
  const db = makeDatabase(sql);
  yield* db.execute(db.client.insertInto("recovery_gate").values({ id: Model.RecoveryGateId.make(1), ready: false }));
});
const holdBrokerAccepted = Effect.gen(function*() {
  const { sql } = yield* BrokerPostgres;
  const db = makeDatabase(sql);
  let after: Model.TasksId | null = null;
  for (;;) {
    const page: ReadonlyArray<Model.Tasks> = yield* db.execute(db.client.selectFrom("tasks").selectAll().$if(after !== null, (query) => query.where("id", ">", after!)).orderBy("id").limit(128));
    if (!page.length) break;
    for (const row of page) {
      const task = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Task), { onExcessProperty: "error" })(row.data);
      if (task.state === "ACCEPTED" || task.state === "RUNNING") {
        const update = { data: Schema.encodeSync(Schema.fromJsonString(Task))({ ...task, state: "HELD", reason: "private recovery: accepted native outcome uncertain; operator journal reconciliation required; no replay" }) };
        const changed = yield* db.execute(db.client.updateTable("tasks").set(update).where("id", "=", row.id).returning("id"));
        if (changed.length !== 1) return yield* recoveryError("Recovered task vanished; target remains fenced");
      }
      after = row.id;
    }
  }
});
const openBrokerGate = Effect.gen(function*() {
  const { sql } = yield* BrokerPostgres;
  const db = makeDatabase(sql);
  const changed = yield* db.execute(db.client.updateTable("recovery_gate").set({ ready: true }).where("id", "=", Model.RecoveryGateId.make(1)).where("ready", "=", false).returningAll());
  if (changed.length !== 1) return yield* recoveryError("Recovery fence missing; refuse opening target");
});
export const restoreBrokerDatabase = (config: BrokerConfig, source: string, holdAcceptedWork = false) => Effect.scoped(Effect.gen(function*() {
  const database = yield* BrokerPostgres;
  return yield* Effect.gen(function*() {
    yield* privateRecoveryPath(source, "file");
    yield* emptyBrokerDatabase;
    yield* validateBrokerDump(config, source, false);
    yield* fenceBroker;
    yield* nativePostgres(config, "/usr/bin/pg_restore", ["--data-only", "--no-owner", "--no-privileges", "--single-transaction", "--exit-on-error", source]);
    const state = yield* inspectBrokerDatabase();
    if (state.pendingAccepted && !holdAcceptedWork) return yield* recoveryError("Restore requires explicit accepted-work HELD acknowledgment");
    yield* holdBrokerAccepted;
    yield* openBrokerGate;
  }).pipe(Effect.raceFirst(database.failure));
})).pipe(Effect.provide(BrokerPostgres.layer(config.databaseUrlFile)), io);
const recoveryStat = (file: string) => Effect.gen(function*() {
  const platform = yield* Platform;
  // Effect FileSystem.stat follows links; recovery needs metadata of the link itself.
  const output = yield* platform.exec(ChildProcess.make("/usr/bin/stat", ["--printf=%f %u %g %h", "--", file], { env: {}, extendEnv: false }), Infinity);
  const match = /^([a-f0-9]+) ([0-9]+) ([0-9]+) ([0-9]+)$/.exec(output);
  if (!match) return yield* recoveryError("Invalid recovery filesystem metadata");
  return yield* Schema.decodeUnknownEffect(RecoveryStat)({ mode: Number.parseInt(match[1]!, 16), uid: Number(match[2]), gid: Number(match[3]), nlink: Number(match[4]) });
}).pipe(io);
const durableCopy = (source: string, destination: string) => Effect.gen(function*() {
  const { fs, path } = yield* Platform;
  // Copy bytes only: copyFile fails on foreign-UID sources without FOWNER.
  yield* Stream.run(fs.stream(source), fs.sink(destination, { flag: "wx", mode: 0o600 }));
  yield* fs.chmod(destination, 0o600);
  yield* syncPath(destination);
  yield* syncPath(path.dirname(destination));
}).pipe(io);
const inspectRecoveryDatabase = (file: string, pairedRunner?: RunnerConfig) => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const { sql } = yield* Sqlite;
  const integrity = yield* sql.unsafe<{ integrity_check: string }>("PRAGMA integrity_check");
  if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") return yield* recoveryError("Invalid SQLite runner recovery artifact");
  const schema = JSON.stringify(yield* sql.unsafe("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"));
  const version = yield* sql.unsafe("PRAGMA user_version");
  const migrations = yield* sql.unsafe("SELECT migration_id,name FROM coordinator_runner_migrations ORDER BY migration_id");
  if (JSON.stringify(migrations) !== JSON.stringify([{ migration_id: 1, name: "relational_baseline" }, { migration_id: 2, name: "native_mcp_binding" }])) return yield* recoveryError("Incompatible runner recovery migrations");
  let after = 0, count = 0, pendingAccepted = 0;
  for (;;) {
    const page = yield* sql.unsafe(`SELECT ${pairedRunner?.childUidBase ?? 1000000}+rowid AS childUid,* FROM tasks WHERE rowid > ? ORDER BY rowid LIMIT 128`, [after]);
    if (!page.length) break;
    for (const row of page) {
      const decoded = yield* Schema.decodeUnknownEffect(RunnerRow, { onExcessProperty: "error" })(row);
      const work = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Work), { onExcessProperty: "error" })(decoded.work);
      if (decoded.taskId !== work.taskId || decoded.fingerprint !== (yield* platform.fingerprint(work))) return yield* recoveryError("Runner recovery task identity/fingerprint mismatch");
      if (pairedRunner && decoded.worktree !== platform.path.join(pairedRunner.childDirectory, yield* platform.digest(decoded.taskId), "repo")) return yield* recoveryError("Runner worktree is outside its canonical task workspace");
      if (decoded.pid !== null && decoded.pid !== undefined) yield* Effect.try({ try: () => {
        try { process.kill(decoded.pid!, 0); }
        catch (error) { if (typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH") return; throw error; }
        throw recoveryError("Recorded native child is live; stop it offline before backup or restore");
      }, catch: () => recoveryError("Native child liveness unsafe or unknown") });
      if (decoded.state === "ACCEPTED" && decoded.occupied === 1 && decoded.detached === 0) pendingAccepted++;
      after = decoded.childUid - (pairedRunner?.childUidBase ?? 1000000);
      count++;
    }
  }
  return { schema: JSON.stringify({ schema, version, migrations }), rows: count, pendingAccepted };
}).pipe(Effect.provide(Sqlite.layer({ filename: file, readonly: true }))));

const recoverOwned = (operation: "backup" | "restore", config: BrokerConfig, artifact: string, sourceSha: Sha, runner?: RunnerConfig, holdAcceptedWork = false) => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  const { fs, path: paths } = platform;
  yield* privateRecoveryPath(config.databaseUrlFile, "file");
  const database = yield* BrokerPostgres;
  if (operation === "restore") yield* emptyBrokerDatabase;
  const locks: Array<{ readonly exitCode: Effect.Effect<number, unknown> }> = [];
  if (runner) {
    // /work is 0711 for isolated child traversal; runner state itself remains private 0700.
    yield* checkedRecoveryPath(runner.stateDirectory, "directory", false, true);
    if (!paths.isAbsolute(runner.childDirectory) || paths.resolve(runner.childDirectory) !== runner.childDirectory ||
      (yield* platform.fs.realPath(runner.childDirectory)) !== runner.childDirectory) return yield* recoveryError("Canonical child directory required");
    if (operation === "restore" && (yield* platform.fs.readDirectory(runner.childDirectory)).length) return yield* recoveryError("Fresh empty child directory required");
    if ((yield* platform.fs.readDirectory(runner.stateDirectory)).some((name) => !["runner.sqlite", "runner.sqlite-wal", "runner.sqlite-shm", "runner.lock"].includes(name))) return yield* recoveryError("Unexpected runner state files; cannot omit required state");
    const lock = paths.join(runner.stateDirectory, "runner.lock");
    yield* privateRecoveryPath(lock, "file", !(yield* platform.fs.exists(lock)));
    locks.push(yield* platform.acquireJournalOwnership(lock));
    yield* privateRecoveryPath(paths.join(runner.stateDirectory, "runner.sqlite"), "file", operation === "restore");
  }
  const work = Effect.gen(function*() {
    const runnerDatabase = runner ? paths.join(runner.stateDirectory, "runner.sqlite") : undefined;
    const roots = runner ? [runner.childDirectory, ...Object.values(runner.repositories)].sort() : [];
    const layout = runner ? JSON.stringify({ stateDirectory: runner.stateDirectory, childDirectory: runner.childDirectory, repositories: runner.repositories, childUidBase: runner.childUidBase }) : "";
    if (roots.some((root, index) => root === "/" || !paths.isAbsolute(root) || paths.resolve(root) !== root ||
      roots.some((other, otherIndex) => index !== otherIndex && (root === other || root.startsWith(`${other}/`))) ||
      [artifact, config.databaseUrlFile, config.backupDirectory, config.objectBackup?.accessKeyFile, config.objectBackup?.secretKeyFile, runner?.stateDirectory, runner?.modelDirectory, runner?.tokenFile].some((privatePath) => privatePath && (privatePath === root || privatePath.startsWith(`${root}/`))))) return yield* recoveryError("Canonical disjoint runner recovery roots required");
    const inside = (file: string) => roots.some((root) => file === root || file.startsWith(`${root}/`));
    for (const root of roots) {
      const info = yield* io(fs.stat(root));
      if (info.type !== "Directory" || (yield* io(fs.realPath(root))) !== root || Option.isNone(info.uid) || info.uid.value !== process.getuid?.() || (info.mode & 0o022) !== 0) return yield* recoveryError("Unsafe runner recovery root");
    }
    if (operation === "backup") {
      yield* privateRecoveryPath(artifact, "directory", true);
      yield* io(fs.makeDirectory(artifact, { mode: 0o700 }));
      const backup = (source: string, name: string) => Effect.gen(function*() {
        const state = yield* inspectRecoveryDatabase(source, runner);
        const destination = paths.join(artifact, name);
        yield* io(fs.writeFileString(destination, "", { flag: "wx", mode: 0o600 }));
        yield* Effect.scoped(Effect.gen(function*() {
          const sqlite = yield* Sqlite;
          yield* sqlite.backup(destination);
        }).pipe(Effect.provide(Sqlite.layer({ filename: source, readonly: true }))));
        yield* Effect.scoped(Effect.gen(function*() {
          const { sql } = yield* Sqlite;
          yield* sql.unsafe("PRAGMA journal_mode=DELETE");
          yield* sql.unsafe("PRAGMA synchronous=FULL");
        }).pipe(Effect.provide(Sqlite.layer({ filename: destination, disableWAL: true }))));
        yield* syncPath(destination);
        yield* noSidecars(destination);
        return { ...state, sha256: yield* hashFile(destination) };
      });
      if (runner && runnerDatabase) yield* validateBrokerRunnerPair(runner, runnerDatabase);
      const brokerState = yield* inspectBrokerDatabase(runner);
      const brokerDump = paths.join(artifact, "broker.pgdump");
      yield* dumpBrokerDatabase(config, brokerDump, false);
      const broker = { ...brokerState, sha256: yield* hashFile(brokerDump) };
      const runnerFile = runnerDatabase ? yield* backup(runnerDatabase, "runner.sqlite") : undefined;
      const entries: Array<typeof RecoveryEntry.Type> = [];
      if (runner) {
        yield* io(fs.makeDirectory(paths.join(artifact, "blobs"), { mode: 0o700 }));
        const visit = (file: string): Effect.Effect<void, BackupError, Platform> => Effect.gen(function*() {
          const relative = paths.relative(runner.childDirectory, file);
          if (/^[^/]+\/(?:native\.token|native-plugin\.json|native-endpoint\.json)$/.test(relative) ||
            /^[^/]+\/home\/\.(?:omp|pi)\/agent\/(?:auth\.json|models\.json|config\.yml|settings\.json)$/.test(relative)) return;
          const info = yield* recoveryStat(file);
          const base = { path: file, uid: info.uid, gid: info.gid, mode: info.mode & 0o777 };
          if ((info.mode & 0o170000) === 0o040000) {
            entries.push({ ...base, type: "directory" });
            for (const name of (yield* io(fs.readDirectory(file))).sort()) yield* visit(paths.join(file, name));
          } else if ((info.mode & 0o170000) === 0o120000) {
            const target = yield* io(fs.readLink(file));
            if (!inside(paths.resolve(paths.dirname(file), target))) return yield* recoveryError("Runner symlink escapes authorized recovery roots");
            entries.push({ ...base, type: "symlink", target });
          } else if ((info.mode & 0o170000) === 0o100000 && info.nlink === 1) {
            if (paths.basename(file) === ".git" || file.endsWith("/objects/info/alternates") || /\/\.git\/(?:commondir|worktrees\/[^/]+\/gitdir)$/.test(file)) {
              const text = yield* io(fs.readFileString(file));
              const references = paths.basename(file) === ".git" ? [text.replace(/^gitdir: /, "").trim()] : text.trim().split("\n");
              const referenceBase = file.endsWith("/objects/info/alternates") ? paths.dirname(paths.dirname(file)) : paths.dirname(file);
              if (references.some((reference) => !reference || !inside(paths.resolve(referenceBase, reference)))) return yield* recoveryError("Git metadata references unbacked external state");
            }
            const blob = String(entries.length), destination = paths.join(artifact, "blobs", blob);
            yield* durableCopy(file, destination);
            entries.push({ ...base, type: "file", blob, sha256: yield* hashFile(destination) });
          } else return yield* recoveryError("Runner state contains unsupported special file or hardlink");
        });
        for (const root of roots) yield* visit(root);
      }
      const metadata: RecoveryMetadata = { kind: "coordinator-private-recovery", version: 2, sourceSha, createdAt: new Date().toISOString(), broker,
        ...(runner && runnerFile ? { runner: { ...runnerFile, id: runner.id, layout, entries } } : {}),
        excluded: ["operator configs and enrollment/control tokens", "model seed and known copied child model config/auth files", "native control token/plugin/endpoint files (no native reattachment; retained journals remain uncertain)", "external host plugin journals/history (operator-owned; accepted broker work is restored HELD, never resumed)"] };
      yield* Effect.scoped(Effect.gen(function*() {
        const file = yield* fs.open(paths.join(artifact, "recovery.json"), { flag: "wx", mode: 0o600 });
        yield* file.writeAll(utf8.encode(yield* Schema.encodeEffect(Schema.fromJsonString(RecoveryMetadata))(metadata)));
        yield* file.sync;
        yield* syncPath(artifact);
      })).pipe(io);
      return metadata;
    }
    if (runnerDatabase) yield* noSidecars(runnerDatabase);
    yield* privateRecoveryPath(artifact, "directory");
    const manifest = paths.join(artifact, "recovery.json");
    yield* privateRecoveryPath(manifest, "file");
    const metadata = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RecoveryMetadata), { onExcessProperty: "error" })(yield* platform.fs.readFileString(manifest));
    if (metadata.broker.pendingAccepted + (metadata.runner?.pendingAccepted ?? 0) > 0 && !holdAcceptedWork) return yield* recoveryError("Restore requires --hold-accepted-work: accepted/running work must be fenced HELD without replay");
    if (metadata.sourceSha !== sourceSha || !!metadata.runner !== !!runner || (metadata.runner && (metadata.runner.id !== runner?.id || metadata.runner.layout !== layout))) return yield* recoveryError("Recovery source revision or runner absolute-path/UID pairing mismatch");
    const expected = ["broker.pgdump", "recovery.json", ...(metadata.runner ? ["runner.sqlite", "blobs"] : [])].sort();
    if (JSON.stringify((yield* platform.fs.readDirectory(artifact)).sort()) !== JSON.stringify(expected)) return yield* recoveryError("Unexpected recovery artifact contents");
    const brokerDump = paths.join(artifact, "broker.pgdump");
    yield* privateRecoveryPath(brokerDump, "file");
    if ((yield* hashFile(brokerDump)) !== metadata.broker.sha256 || (yield* brokerSchemaIdentity(database.sql)) !== metadata.broker.schema) return yield* recoveryError("Broker recovery digest/schema mismatch");
    const sources = metadata.runner && runnerDatabase ? [{ source: paths.join(artifact, "runner.sqlite"), destination: runnerDatabase, metadata: metadata.runner, runner: true }] : [];
    // Validate the whole pair before writing either fresh destination.
    for (const item of sources) {
      yield* privateRecoveryPath(item.source, "file");
      yield* noSidecars(item.source);
      if ((yield* hashFile(item.source)) !== item.metadata.sha256) return yield* recoveryError("Recovery artifact digest mismatch");
      const inspected = yield* inspectRecoveryDatabase(item.source, runner);
      if (inspected.schema !== item.metadata.schema || inspected.rows !== item.metadata.rows || inspected.pendingAccepted !== item.metadata.pendingAccepted) return yield* recoveryError("Recovery schema/state mismatch");
    }
    if (metadata.runner) {
      const seen = new Set<string>();
      const directories = new Set<string>();
      const blobs = new Set<string>();
      yield* privateRecoveryPath(paths.join(artifact, "blobs"), "directory");
      for (const entry of metadata.runner.entries) {
        if (!inside(entry.path) || paths.resolve(entry.path) !== entry.path || seen.has(entry.path) ||
          (entry.mode & ~0o777) !== 0 || entry.uid < 0 || entry.gid < 0) return yield* recoveryError("Unsafe runner recovery manifest");
        seen.add(entry.path);
        if (!roots.includes(entry.path) && !directories.has(paths.dirname(entry.path))) return yield* recoveryError("Runner recovery parent missing, non-directory, or unordered");
        if (entry.type === "directory") directories.add(entry.path);
        if (entry.type === "symlink" && (!entry.target || !inside(paths.resolve(paths.dirname(entry.path), entry.target)))) return yield* recoveryError("Unsafe restored symlink");
        if (entry.type === "file") {
          if (!entry.blob || !entry.sha256 || blobs.has(entry.blob)) return yield* recoveryError("Missing or duplicate runner file digest/blob");
          blobs.add(entry.blob);
          const blob = paths.join(artifact, "blobs", entry.blob);
          yield* privateRecoveryPath(blob, "file");
          if ((yield* hashFile(blob)) !== entry.sha256) return yield* recoveryError("Runner blob digest mismatch");
        }
      }
      if (JSON.stringify((yield* io(fs.readDirectory(paths.join(artifact, "blobs")))).sort()) !== JSON.stringify([...blobs].sort())) return yield* recoveryError("Unexpected runner recovery blobs");
      for (const root of roots) {
        if (!directories.has(root) || (yield* io(fs.readDirectory(root))).length) return yield* recoveryError("Fresh empty runner child/repository roots required");
      }
    }
    yield* validateBrokerDump(config, brokerDump, true);
    yield* fenceBroker;
    yield* nativePostgres(config, "/usr/bin/pg_restore", ["--data-only", "--no-owner", "--no-privileges", "--single-transaction", "--exit-on-error", brokerDump]);
    const inspectedBroker = yield* inspectBrokerDatabase(runner);
    if (inspectedBroker.rows !== metadata.broker.rows || inspectedBroker.pendingAccepted !== metadata.broker.pendingAccepted || inspectedBroker.schema !== metadata.broker.schema) return yield* recoveryError("Restored broker state mismatch; target remains fenced");
    if (runner && metadata.runner) yield* validateBrokerRunnerPair(runner, paths.join(artifact, "runner.sqlite"));
    yield* holdBrokerAccepted;
    for (const item of sources) yield* durableCopy(item.source, item.destination);
    if (runnerDatabase && metadata.runner && metadata.runner.pendingAccepted > 0) yield* Effect.scoped(Effect.gen(function*() {
      const { sql } = yield* Sqlite;
      yield* sql.unsafe("PRAGMA synchronous=FULL");
      yield* sql.withTransaction(sql.unsafe("UPDATE tasks SET state='HELD',reason='private recovery: accepted launch fenced; operator reconciliation required; no replay' WHERE state='ACCEPTED' AND occupied=1 AND detached=0"));
      yield* sql.unsafe("PRAGMA wal_checkpoint(TRUNCATE)");
    }).pipe(Effect.provide(Sqlite.layer({ filename: runnerDatabase }))));
    if (metadata.runner) {
      for (const entry of metadata.runner.entries) {
        if (entry.type === "directory") {
          if (!roots.includes(entry.path)) yield* io(fs.makeDirectory(entry.path, { mode: 0o700 }));
        } else if (entry.type === "symlink") yield* io(fs.symlink(entry.target!, entry.path));
        else yield* io(Stream.run(fs.stream(paths.join(artifact, "blobs", entry.blob!)), fs.sink(entry.path, { flag: "wx", mode: 0o600 })));
      }
      // Finish modes while operator-owned: the supervisor has CHOWN, not FOWNER.
      // Reverse order keeps parents traversable until their descendants are finalized.
      for (const entry of [...metadata.runner.entries].reverse()) {
        if (entry.type === "symlink") {
          // Effect chown follows links; the fixed helper changes only symlink ownership.
          yield* io(platform.exec(ChildProcess.make("/usr/bin/chown", ["--no-dereference", `${entry.uid}:${entry.gid}`, "--", entry.path], { env: {}, extendEnv: false })));
        } else yield* Effect.scoped(Effect.gen(function*() {
          const file = yield* fs.open(entry.path);
          yield* fs.chmod(entry.path, entry.mode);
          yield* file.sync;
          yield* fs.chown(entry.path, entry.uid, entry.gid);
          yield* file.sync;
        })).pipe(io);
      }
    }
    yield* openBrokerGate;
    return metadata;
  });
  const lost = (lock: (typeof locks)[number]) => lock.exitCode.pipe(Effect.catch(() => Effect.succeed(undefined)), Effect.andThen(Effect.fail(recoveryError("Recovery ownership lost; fence partial output and do not start services"))));
  return yield* locks.reduce((effect, lock) => effect.pipe(Effect.raceFirst(lost(lock))), work).pipe(Effect.raceFirst(database.failure));
})).pipe(io);
export const recover = (operation: "backup" | "restore", config: BrokerConfig, artifact: string, sourceSha: Sha, runner?: RunnerConfig, holdAcceptedWork = false) =>
  recoverOwned(operation, config, artifact, sourceSha, runner, holdAcceptedWork).pipe(Effect.provide(BrokerPostgres.layer(config.databaseUrlFile)));

// Frozen supported offline SQLite contracts, not a second live broker implementation.
export const legacyBrokerSql = (queueVersion: 1 | 2): ReadonlyArray<string> => [
  "CREATE TABLE tasks (id TEXT PRIMARY KEY, data TEXT NOT NULL)",
  "CREATE TABLE sessions (id TEXT PRIMARY KEY, data TEXT NOT NULL)",
  "CREATE TABLE peers (id TEXT PRIMARY KEY, owner TEXT NOT NULL, sender TEXT NOT NULL, session TEXT NOT NULL, incarnation TEXT NOT NULL, payload TEXT NOT NULL, fingerprint TEXT NOT NULL, expires REAL NOT NULL)",
  "CREATE TABLE consents (owner TEXT NOT NULL, session TEXT NOT NULL, enabled INTEGER NOT NULL, participants TEXT NOT NULL, PRIMARY KEY(owner,session))",
  "CREATE TABLE participant_consents (owner TEXT NOT NULL, session TEXT NOT NULL, participant TEXT NOT NULL, enabled INTEGER NOT NULL, PRIMARY KEY(owner,session,participant))",
  "CREATE TABLE archive (sequence INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, session TEXT NOT NULL, entry TEXT NOT NULL, digest TEXT NOT NULL, payload TEXT NOT NULL, gaps TEXT NOT NULL, created REAL NOT NULL, UNIQUE(owner,session,entry,digest))",
  "CREATE TABLE evidence (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT NOT NULL, kind TEXT NOT NULL, sha TEXT NOT NULL, principal TEXT NOT NULL, data TEXT NOT NULL, observed REAL NOT NULL)",
  ...["coordinator_broker_migrations", "effect_queue_migrations"].map((name) => `CREATE TABLE ${name} (migration_id integer PRIMARY KEY NOT NULL, created_at datetime NOT NULL DEFAULT current_timestamp, name VARCHAR(255) NOT NULL)`),
  queueVersion === 1 ?
    "CREATE TABLE effect_queue (sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL, queue_name TEXT NOT NULL, element TEXT NOT NULL, completed BOOLEAN NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, last_failure TEXT NULL, acquired_at DATETIME NULL, acquired_by TEXT NULL, created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL)" :
    "CREATE TABLE effect_queue (sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL, queue_name TEXT NOT NULL, element TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, last_failure TEXT NULL, visible_at DATETIME NOT NULL, acquired_at DATETIME NULL, acquired_by TEXT NULL, created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL)",
  "CREATE UNIQUE INDEX idx_effect_queue_id ON effect_queue (id, queue_name)",
  "CREATE INDEX idx_effect_queue_update ON effect_queue (sequence, acquired_by)",
  queueVersion === 1 ? "CREATE INDEX idx_effect_queue_take ON effect_queue (queue_name, completed, attempts, acquired_at)" :
    "CREATE INDEX idx_effect_queue_take ON effect_queue (queue_name, visible_at) WHERE state = 'pending'",
];

export const importLegacySqlite = (source: string, config: BrokerConfig, artifact: string, sourceSha: Sha, runner?: RunnerConfig, holdAcceptedWork = false) => Effect.scoped(Effect.gen(function*() {
  const platform = yield* Platform;
  if (runner) {
    if ([runner.childDirectory, ...Object.values(runner.repositories)].some((root) => source === root || source.startsWith(`${root}/`))) return yield* recoveryError("Legacy source must remain outside archived runner roots");
  }
  yield* privateRecoveryPath(source, "file");
  yield* privateRecoveryPath(`${source}.lock`, "file", !(yield* platform.fs.exists(`${source}.lock`)));
  const sourceLock = yield* platform.acquireJournalOwnership(`${source}.lock`);
  for (const suffix of ["-wal", "-shm", "-journal"]) {
    yield* privateRecoveryPath(`${source}${suffix}`, "file", !(yield* platform.fs.exists(`${source}${suffix}`)));
  }
  // Committed WAL is checkpointed under source flock before enforcing the standalone no-sidecars contract.
  // SQLite's own exclusive journal-mode transition fails if another uncooperative writer remains.
  yield* Effect.scoped(Effect.gen(function*() {
    const { sql } = yield* Sqlite;
    yield* sql.unsafe("PRAGMA synchronous=FULL");
    const checkpoint = yield* sql.unsafe<{ busy: number }>("PRAGMA wal_checkpoint(TRUNCATE)");
    if (checkpoint.some((row) => row.busy !== 0)) return yield* recoveryError("Legacy WAL checkpoint busy; stop every source writer offline");
    const mode = yield* sql.unsafe<{ journal_mode: string }>("PRAGMA journal_mode=DELETE");
    if (mode[0]?.journal_mode !== "delete") return yield* recoveryError("Legacy standalone checkpoint failed");
  }).pipe(Effect.provide(Sqlite.layer({ filename: source, disableWAL: true })), io,
    Effect.mapError((error) => recoveryError(`Legacy checkpoint: ${error.message}`))));
  yield* noSidecars(source);
  const sourceDigest = yield* hashFile(source);
  const operation = Effect.scoped(Effect.gen(function*() {
    const { sql: target } = yield* BrokerPostgres;
    const db = makeDatabase(target);
    yield* emptyBrokerDatabase;
    const { sql: legacy } = yield* Sqlite;
    const integrity = yield* legacy.unsafe<{ integrity_check: string }>("PRAGMA integrity_check");
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") return yield* recoveryError("Invalid legacy SQLite integrity");
    const brokerVersions = yield* legacy.unsafe<{ migration_id: number; name: string }>("SELECT migration_id,name FROM coordinator_broker_migrations ORDER BY migration_id");
    const queueVersions = yield* legacy.unsafe<{ migration_id: number; name: string }>("SELECT migration_id,name FROM effect_queue_migrations ORDER BY migration_id");
    if (JSON.stringify(brokerVersions) !== JSON.stringify([{ migration_id: 1, name: "relational_baseline" }]) ||
      (JSON.stringify(queueVersions) !== JSON.stringify([{ migration_id: 1, name: "create_table" }]) &&
        JSON.stringify(queueVersions) !== JSON.stringify([{ migration_id: 1, name: "create_table" }, { migration_id: 2, name: "upgrade_schema" }]))) return yield* recoveryError("Unsupported legacy broker/queue migration history");
    const version = queueVersions.length === 1 ? 1 : 2;
    const definitions = yield* legacy.unsafe<{ sql: string }>("SELECT sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name");
    const normalize = (value: string) => value.split(/('(?:''|[^'])*')/).map((part, index) => index % 2 ? part : part.replace(/IF NOT EXISTS/gi, "").replace(/["`\[\]\s]/g, "").toUpperCase()).join("");
    if (JSON.stringify(definitions.map((row) => normalize(row.sql)).sort()) !== JSON.stringify(legacyBrokerSql(version).map(normalize).sort())) return yield* recoveryError("Unsupported legacy SQLite schema; no best-effort import");
    const userVersion = yield* legacy.unsafe<{ user_version: number }>("PRAGMA user_version");
    if (![0, 1].includes(userVersion[0]?.user_version ?? -1)) return yield* recoveryError("Unsupported legacy user_version");
    let pendingAccepted = 0, after: string | null = null;
    for (;;) {
      const page: ReadonlyArray<{ id: string; data: string }> = yield* legacy.unsafe<{ id: string; data: string }>("SELECT id,data FROM tasks WHERE (? IS NULL OR id > ?) ORDER BY id LIMIT 128", [after, after]);
      if (!page.length) break;
      for (const row of page) {
        const task = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Task), { onExcessProperty: "error" })(row.data).pipe(
          Effect.mapError(() => recoveryError("Legacy task JSON decode failed")));
        if (task.taskId !== row.id || task.request.taskId !== row.id || task.fingerprint !== (yield* platform.fingerprint(task.request))) return yield* recoveryError("Legacy task identifier/fingerprint mismatch");
        if (task.state === "ACCEPTED" || task.state === "RUNNING") pendingAccepted++;
        if (task.runnerId && !["SUCCEEDED", "FAILED"].includes(task.state) && task.runnerId !== runner?.id) return yield* recoveryError("Legacy assigned uncertainty requires its paired runner");
        after = row.id;
      }
    }
    const runnerState = runner ? yield* inspectRecoveryDatabase(platform.path.join(runner.stateDirectory, "runner.sqlite"), runner) : undefined;
    if ((pendingAccepted + (runnerState?.pendingAccepted ?? 0)) && !holdAcceptedWork) return yield* recoveryError("Import requires --hold-accepted-work; accepted/running work is HELD without replay");
    yield* privateRecoveryPath(artifact, "directory", true);
    yield* fenceBroker;
    yield* target.withTransaction(Effect.gen(function*() {
      for (const table of brokerTables) {
        let cursor: string | null = null, imported = 0;
        for (;;) {
          const page: ReadonlyArray<Record<string, string | number | bigint | null>> = yield* legacy.unsafe<Record<string, string | number | bigint | null>>(`SELECT CAST(rowid AS TEXT) AS "_cursor",* FROM "${table}" WHERE (? IS NULL OR rowid > ?) ORDER BY rowid LIMIT 128`, [cursor, cursor]);
          if (!page.length) break;
          for (const original of page) {
            const { _cursor, ...row } = original;
            if (typeof _cursor !== "string") return yield* recoveryError("Legacy cursor unavailable");
            cursor = _cursor;
            if (table === "sessions") {
              const session = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LiveSession), { onExcessProperty: "error" })(row.data).pipe(
                Effect.mapError(() => recoveryError("Legacy session JSON decode failed")));
              if (session.sessionId !== row.id) return yield* recoveryError("Legacy session identifier mismatch");
            }
            if (table === "effect_queue" && version === 1) {
              if (row.completed !== 0n && row.completed !== 1n) return yield* recoveryError("Invalid legacy queue boolean");
              row.state = row.completed === 1n ? "completed" : "pending";
              row.visible_at = row.updated_at ?? null;
              delete row.completed;
            }
            if (table === "consents" || table === "participant_consents") {
              if (row.enabled !== 0n && row.enabled !== 1n) return yield* recoveryError("Invalid legacy consent boolean");
            }
            const input: Record<string, unknown> = {};
            const expected: Record<string, string | null> = {};
            for (const column of Object.keys(row)) {
              const value = row[column] ?? null;
              if (value !== null && !["string", "number", "bigint"].includes(typeof value)) return yield* recoveryError("Unsupported legacy SQLite cell type");
              const timestamp = table === "effect_queue" && ["visible_at", "acquired_at", "created_at", "updated_at"].includes(column);
              if (timestamp && value !== null && (typeof value !== "string" || !/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d{1,6})?$/.test(value))) return yield* recoveryError("Unsupported native legacy timestamp encoding");
              if (table === "effect_queue" && column === "state" && !["pending", "processing", "completed", "failed"].includes(String(value))) return yield* recoveryError("Unsupported legacy queue state");
              if (table === "effect_queue" && column === "acquired_by" && value !== null && (typeof value !== "string" || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(value))) return yield* recoveryError("Invalid legacy queue owner UUID");
              input[column] = timestamp && typeof value === "string" ? new Date(`${value.replace(" ", "T")}Z`) :
                column === "enabled" ? (value === 1n ? 1 : 0) :
                column === "attempts" && typeof value === "bigint" ? Number(value) : value;
              if (column === "attempts" && !Number.isSafeInteger(input[column])) return yield* recoveryError("Legacy attempts exceed safe integer range");
              expected[column] = value === null ? null : timestamp ? `${String(value).split(".")[0]}.${(String(value).split(".")[1] ?? "").padEnd(6, "0")}` : String(value);
            }
            // Decode every source row with generated insert schemas; returning checks include every stored field.
            const inserted = yield* Effect.gen(function*() {
              switch (table) {
                case "tasks": {
                  const value = yield* Schema.decodeUnknownEffect(Model.TasksInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy tasks insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "sessions": {
                  const value = yield* Schema.decodeUnknownEffect(Model.SessionsInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy sessions insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "peers": {
                  const value = yield* Schema.decodeUnknownEffect(Model.PeersInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy peers insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "consents": {
                  const value = yield* Schema.decodeUnknownEffect(Model.ConsentsInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy consents insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "participant_consents": {
                  const value = yield* Schema.decodeUnknownEffect(Model.ParticipantConsentsInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy participant_consents insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "archive": {
                  const value = yield* Schema.decodeUnknownEffect(Model.ArchiveInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy archive insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "evidence": {
                  const value = yield* Schema.decodeUnknownEffect(Model.EvidenceInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy evidence insert decode failed")));
                  return yield* db.execute(db.client.insertInto(table).values(value).returningAll());
                }
                case "effect_queue": {
                  const value = yield* Schema.decodeUnknownEffect(Model.EffectQueueInsert, { onExcessProperty: "error" })(input).pipe(
                    Effect.mapError(() => recoveryError("Legacy effect_queue insert decode failed")));
                  // Keep original PostgreSQL microseconds, not Date's millisecond-truncated validation value.
                  yield* db.execute(db.client.insertInto(table).values({ ...value,
                    acquired_at: expression<Date | null>`${row.acquired_at}::timestamp`,
                    created_at: expression<Date>`${row.created_at}::timestamp`,
                    updated_at: expression<Date>`${row.updated_at}::timestamp`,
                    visible_at: expression<Date>`${row.visible_at}::timestamp`,
                  }));
                  if (value.sequence === undefined) return yield* recoveryError("Legacy queue sequence unavailable");
                  return yield* db.execute(db.client.selectFrom(table).select(["sequence", "id", "queue_name", "element", "state", "attempts", "last_failure", "acquired_by"]).select((eb) => [
                    eb.fn<string | null>("to_char", [eb.ref("acquired_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("acquired_at"),
                    eb.fn<string>("to_char", [eb.ref("created_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("created_at"),
                    eb.fn<string>("to_char", [eb.ref("updated_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("updated_at"),
                    eb.fn<string>("to_char", [eb.ref("visible_at"), eb.val("YYYY-MM-DD HH24:MI:SS.US")]).as("visible_at"),
                  ]).where("sequence", "=", value.sequence));
                }
              }
            });
            const checkedRow = inserted[0];
            const actual = checkedRow === undefined ? {} : Object.fromEntries(Object.entries(checkedRow).map(([column, value]) =>
              [column, value === null ? null : typeof value === "boolean" ? (value ? "1" : "0") : String(value)]));
            if (inserted.length !== 1 || (yield* platform.fingerprint(actual)) !== (yield* platform.fingerprint(expected))) return yield* recoveryError("Legacy row changed during exact import");
            imported++;
          }
        }
        const counts = yield* db.execute(db.client.selectFrom(table).select((eb) => eb.fn.countAll<string>().as("count")));
        const sourceCounts = yield* legacy.unsafe<{ count: bigint }>(`SELECT count(*) AS count FROM "${table}"`);
        if (sourceCounts[0]?.count !== BigInt(imported)) return yield* recoveryError("Legacy source row count mismatch");
        if (BigInt(counts[0]?.count ?? "-1") !== BigInt(imported)) return yield* recoveryError("Legacy table row count mismatch");
      }
      const sequences: ReadonlyArray<readonly ["archive" | "evidence" | "effect_queue", "sequence" | "id"]> = [["archive", "sequence"], ["evidence", "id"], ["effect_queue", "sequence"]];
      for (const [table, column] of sequences) {
        const floors = yield* legacy.unsafe<{ seq: bigint }>("SELECT seq FROM sqlite_sequence WHERE name=?", [table]);
        const maximum = table === "evidence" ?
          yield* db.execute(db.client.selectFrom("evidence").select((eb) => eb.fn.max<Model.EvidenceId | null>("id").as("value"))) :
          yield* db.execute(db.client.selectFrom(table).select((eb) => eb.fn.max<Model.ArchiveId | Model.EffectQueueId | null>("sequence").as("value")));
        const sourceFloor = floors[0]?.seq ?? 0n, highest = BigInt(maximum[0]?.value ?? "0");
        const floor = sourceFloor > highest ? sourceFloor : highest;
        const sequence = yield* db.execute(db.client.selectNoFrom((eb) => eb.fn<string>("pg_get_serial_sequence", [eb.val(table), eb.val(column)]).as("name")));
        if (!sequence[0]?.name || floor < 0n) return yield* recoveryError("Legacy sequence identity/floor invalid");
        yield* db.execute(db.client.selectNoFrom(expression<bigint>`setval(${sequence[0].name}::regclass,${floor > 0n ? floor : 1n}::bigint,${floor > 0n})`.as("value")));
      }
    }).pipe(Effect.provideService(SqlClient.SafeIntegers, true)));
    // All native runner files, permissions, UID layout and no-live-child guards must finish before opening the target gate.
    const metadata = yield* recoverOwned("backup", config, artifact, sourceSha, runner).pipe(
      Effect.mapError((error) => recoveryError(`Legacy paired archive: ${error.message}`)));
    if (metadata.broker.pendingAccepted + (metadata.runner?.pendingAccepted ?? 0) > 0 && !holdAcceptedWork) return yield* recoveryError("Legacy paired acceptance changed; target remains fenced without acknowledgment");
    yield* holdBrokerAccepted;
    if ((yield* hashFile(source)) !== sourceDigest) return yield* recoveryError("Legacy source changed during import; target remains fenced");
    yield* openBrokerGate;
    return metadata;
  }).pipe(Effect.provide(Sqlite.layer({ filename: source, readonly: true })), io,
    Effect.mapError((error) => recoveryError(`Legacy import: ${error.message}`))));
  const lost = sourceLock.exitCode.pipe(Effect.andThen(Effect.fail(recoveryError("Legacy source ownership lost; target must remain fenced"))));
  return yield* operation.pipe(Effect.raceFirst(lost), Effect.raceFirst((yield* BrokerPostgres).failure));
})).pipe(Effect.provide(BrokerPostgres.layer(config.databaseUrlFile)), io);
