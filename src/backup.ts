import { Clock, Context, Effect, FileSystem, Layer, Schema, Stream } from "effect";
import { Hex } from "effect/encoding";
import { HttpClient, HttpClientRequest, type HttpClientResponse } from "effect/http";
import { BackupError, ObjectBackupConfig, SnapshotKey } from "./contracts.ts";
import { Platform } from "./platform.ts";

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
    // ponytail: the caller owns the immutable, sanitized file; stream it rather than copy SQLite into RAM.
    const body = yield* HttpClientRequest.bodyFile(request, file, { contentType: "application/vnd.sqlite3" }).pipe(Effect.provideService(FileSystem.FileSystem, platform.fs));
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
      if (Number(key.slice("sanitized/".length, -".sqlite".length)) >= cutoff) continue;
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
