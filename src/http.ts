import { createServer, type Server } from "node:http";
import { LIMIT, RpcEnvelope, decode } from "./protocol.ts";

export class HttpError extends Error {
  constructor(readonly status: number, message: string) { super(message); }
}
export function serveRpc(options: {
  host: string; port: number;
  authorize: (token: string) => unknown;
  handle: (principal: unknown, method: string, params: unknown) => Promise<unknown>;
}): Promise<Server> {
  const server = createServer(async (request, response) => {
    response.setHeader("content-type", "application/json");
    response.setHeader("cache-control", "no-store");
    try {
      if (request.url === "/healthz" && request.method === "GET") {
        response.end('{"status":"ok"}'); return;
      }
      if (request.url !== "/rpc" || request.method !== "POST") throw new HttpError(404, "Not found");
      const auth = request.headers.authorization;
      if (!auth?.startsWith("Bearer ") || auth.length > 512) throw new HttpError(401, "Credential required");
      const principal = options.authorize(auth.slice(7));
      const chunks: Buffer[] = []; let size = 0;
      for await (const chunk of request) {
        size += Buffer.byteLength(chunk);
        if (size > LIMIT) throw new HttpError(413, "Request too large");
        chunks.push(Buffer.from(chunk));
      }
      const input = decode(RpcEnvelope, JSON.parse(Buffer.concat(chunks).toString("utf8")));
      const result = await options.handle(principal, input.method, input.params);
      const body = JSON.stringify(result ?? null);
      if (Buffer.byteLength(body) > LIMIT) throw new HttpError(413, "Result too large; use cursor pagination");
      response.end(body);
    } catch (error) {
      const status = error instanceof HttpError ? error.status : 400;
      response.statusCode = status;
      response.end(JSON.stringify({ error: error instanceof Error ? error.message.slice(0, 512) : "Request failed" }));
    }
  });
  server.requestTimeout = 15000;
  const { promise, resolve, reject } = Promise.withResolvers<Server>();
  server.once("error", reject);
  server.listen(options.port, options.host, () => resolve(server));
  return promise;
}
