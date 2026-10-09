import { Clock, Context, Effect, FileSystem, Layer, Schema, Stream } from "effect";
import { Hex } from "effect/encoding";
import { HttpClient, HttpClientRequest, type HttpClientResponse } from "effect/http";
import { BackupError, ObjectBackupConfig, SnapshotKey } from "./contracts.ts";
import { decodeSnapshotList } from "./http.ts";
import { Platform } from "./platform.ts";

const utf8 = new TextEncoder();
const encode = (value: string) => encodeURIComponent(value).replace(/[!'()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
const failure = () => new BackupError({ message: "Snapshot object storage operation failed" });
const checked = <A>(f: () => A) => Effect.try({ try: f, catch: failure });

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
