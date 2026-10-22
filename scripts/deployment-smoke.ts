import { Console, Crypto, Effect, Layer, Option, Schema } from "effect";
import { Command, Flag } from "effect/cli";
import { Headers, HttpClient, HttpClientRequest } from "effect/http";
import { NodeHttpClient, NodeRuntime, NodeServices } from "@effect/platform-node";
import { HttpError, InspectionTools, SessionRef, sessionRefKey } from "../src/inspection.ts";
import { Mcp } from "../src/mcp.ts";
import { loadConnectorConfig, readConnectorToken, readPrivateFile } from "../src/native.ts";
import { Host } from "../src/platform.ts";

const help = `Usage: deployment-smoke [--config <private ConnectorConfig>] --source-sha <full40hex> [--safe-target <private SessionRef.json>]
Strictly read-only TLS/hostname, authentication and readiness acceptance.
Without --safe-target, no authenticated native session list or detail getter is called.
The explicit target authorizes same-owner discovery and inspection of only that native session.
No connector enrollment, presence, task, stop, native control or cleanup writes occur.
HTTPS is mandatory. The platform trust store (including NODE_EXTRA_CA_CERTS) verifies TLS and hostname.
No credentials, native identifiers, config paths or observation payloads are printed.
URL and credentials use the shared config loader, including COORDINATOR_API_URL / COORDINATOR_API_TOKEN.
Never pass raw credentials as arguments.
`;
const fail = (message: string) => Effect.fail(new HttpError({ status: 400, message }));
const program = Command.make("deployment-smoke", {
  config: Flag.String("config").pipe(Flag.optional),
  sourceSha: Flag.String("source-sha").pipe(Flag.optional),
  safeTarget: Flag.String("safe-target").pipe(Flag.optional),
}, (flags) => Effect.gen(function*() {
  const configFile = Option.getOrUndefined(flags.config), sourceSha = Option.getOrUndefined(flags.sourceSha);
  if (sourceSha === undefined || !/^[a-f0-9]{40}$/.test(sourceSha)) return yield* fail("Full source SHA required");
  const config = yield* loadConnectorConfig(configFile);
  if (config === undefined) return yield* fail("Inspector URL configuration required");
  const endpoint = new URL(config.inspectorUrl);
  if (endpoint.protocol !== "https:" || endpoint.pathname !== "/") return yield* fail("HTTPS inspector authority required");
  const token = yield* readConnectorToken(config);
  const mcp = yield* Mcp;
  const crypto = yield* Crypto.Crypto;
  const invalidToken = `invalid-${yield* crypto.randomUUIDv4()}`;
  const client = HttpClient.withScope(yield* HttpClient.HttpClient);
  const probe = (path: string, credential?: string) => Effect.gen(function*() {
    const headers = {
      "content-type": "application/json",
      ...(credential === undefined ? {} : { authorization: `Bearer ${credential}` }),
    };
    const url = new URL(path, endpoint);
    const request = path === "/mcp"
      ? HttpClientRequest.post(url, { headers }).pipe(HttpClientRequest.bodyText("{}", "application/json"))
      : HttpClientRequest.get(url, { headers });
    const response = yield* client.execute(request);
    return { status: response.status, sourceSha: Option.getOrUndefined(Headers.get(response.headers, "x-coordinator-source-sha")) };
  }).pipe(
    Effect.scoped,
    Effect.provideService(NodeHttpClient.RequestInit, { redirect: "error" }),
    Effect.mapError(() => new HttpError({ status: 502, message: "Verified TLS HTTP probe failed" })),
    Effect.timeout("10 seconds"),
  );
  for (const credential of [undefined, invalidToken]) {
    for (const path of ["/readyz", "/mcp"]) {
      const response = yield* probe(path, credential);
      if (response.status !== 401 && response.status !== 403) return yield* fail("HTTP authentication rejection not proven");
    }
  }
  const ready = yield* probe("/readyz", token);
  if (ready.status !== 200 || ready.sourceSha !== sourceSha) return yield* fail("Readiness source identity mismatch");
  let targetInspected = false;
  const targetFile = Option.getOrUndefined(flags.safeTarget);
  if (targetFile !== undefined) {
    const ref = yield* Schema.decodeEffect(Schema.fromJsonString(SessionRef), { onExcessProperty: "error" })(yield* readPrivateFile(targetFile));
    let cursor: string | undefined;
    let found = false;
    for (let page = 0; page < 10; page++) {
      const result = yield* mcp.call(config.inspectorUrl, token, InspectionTools.list, {
        machineId: ref.machineId, harness: ref.harness, limit: 100, ...(cursor === undefined ? {} : { cursor }),
      });
      if (result.sessions.some((presence) => sessionRefKey(presence.ref) === sessionRefKey(ref))) { found = true; break; }
      if (result.nextCursor === null) break;
      cursor = result.nextCursor;
    }
    if (!found) return yield* fail("Explicit safe target is not discoverable by this owner");
    const observation = yield* mcp.call(config.inspectorUrl, token, InspectionTools.inspect, { ref });
    if (sessionRefKey(observation.presence.ref) !== sessionRefKey(ref)) return yield* fail("Inspection target mismatch");
    targetInspected = true;
  }
  yield* Console.log(JSON.stringify({ scenario: "deployment-acceptance", status: "ok", sourceSha, verifiedTlsAndHostname: true,
    authenticationRejected: true, readinessSourceMatched: true, explicitSafeTargetInspected: targetInspected, readOnly: true }));
})).pipe(Command.withDescription(help));

NodeRuntime.runMain(Command.run(program, { version: "0.2.0-beta.5", renderErrors: false }).pipe(
  Effect.scoped,
  Effect.timeout("60 seconds"),
  Effect.catchCause((cause) => Console.error(JSON.stringify({
    scenario: "deployment-acceptance", status: "failed", error: "Acceptance failed; private diagnostics withheld",
  })).pipe(Effect.andThen(Effect.failCause(cause)))),
  Effect.provide(Layer.mergeAll(NodeServices.layer, Host.layer, Mcp.layer, NodeHttpClient.layerFetch)),
), { disableErrorReporting: true });
