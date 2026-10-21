import { randomUUID } from "node:crypto";
import { parseArgs } from "node:util";
import { Effect, Layer, Schema } from "effect";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { HttpError, InspectionTools, SessionRef, sessionRefKey } from "../src/inspection.ts";
import { Mcp } from "../src/mcp.ts";
import { loadConnectorConfig, readConnectorToken, readPrivateFile } from "../src/native.ts";

const help = `Usage: deployment-smoke --config <private ConnectorConfig> --source-sha <full40hex> [--safe-target <private SessionRef.json>]
Strictly read-only TLS/hostname, authentication and readiness acceptance.
Without --safe-target, no authenticated native session list or detail getter is called.
The explicit target authorizes same-owner discovery and inspection of only that native session.
No connector enrollment, presence, task, stop, native control or cleanup writes occur.
HTTPS is mandatory. The platform trust store (including NODE_EXTRA_CA_CERTS) verifies TLS and hostname.
No credentials, native identifiers, config paths or observation payloads are printed.
`;
const fail = (message: string) => Effect.fail(new HttpError({ status: 400, message }));
const program = Effect.gen(function*() {
  const flags = yield* Effect.try({
    try: () => parseArgs({ strict: true, allowPositionals: false, options: {
      config: { type: "string" }, "source-sha": { type: "string" }, "safe-target": { type: "string" }, help: { type: "boolean" },
    } }),
    catch: () => new HttpError({ status: 400, message: "Invalid arguments" }),
  });
  if (flags.values.help) { console.log(help); return; }
  const configFile = flags.values.config, sourceSha = flags.values["source-sha"];
  if (configFile === undefined || sourceSha === undefined || !/^[a-f0-9]{40}$/.test(sourceSha)) return yield* fail("Private config and full source SHA required");
  const config = yield* loadConnectorConfig(configFile);
  if (config === undefined) return yield* fail("Explicit connector config required");
  const endpoint = new URL(config.inspectorUrl);
  if (endpoint.protocol !== "https:" || endpoint.pathname !== "/") return yield* fail("HTTPS inspector authority required");
  const token = yield* readConnectorToken(config);
  const mcp = yield* Mcp;
  const invalidToken = `invalid-${randomUUID()}`;
  const probe = (path: string, credential?: string) => Effect.tryPromise({
    try: async (signal) => {
      const response = await fetch(new URL(path, endpoint), { redirect: "error", signal,
        method: path === "/mcp" ? "POST" : "GET",
        headers: { "content-type": "application/json", ...(credential === undefined ? {} : { authorization: `Bearer ${credential}` }) },
        ...(path === "/mcp" ? { body: "{}" } : {}) });
      await response.body?.cancel();
      return { status: response.status, sourceSha: response.headers.get("x-coordinator-source-sha") };
    },
    catch: () => new HttpError({ status: 502, message: "Verified TLS HTTP probe failed" }),
  }).pipe(Effect.timeout("10 seconds"));
  for (const credential of [undefined, invalidToken]) {
    for (const path of ["/readyz", "/mcp"]) {
      const response = yield* probe(path, credential);
      if (response.status !== 401 && response.status !== 403) return yield* fail("HTTP authentication rejection not proven");
    }
  }
  const ready = yield* probe("/readyz", token);
  if (ready.status !== 200 || ready.sourceSha !== sourceSha) return yield* fail("Readiness source identity mismatch");
  let targetInspected = false;
  const targetFile = flags.values["safe-target"];
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
  console.log(JSON.stringify({ scenario: "deployment-acceptance", status: "ok", sourceSha, verifiedTlsAndHostname: true,
    authenticationRejected: true, readinessSourceMatched: true, explicitSafeTargetInspected: targetInspected, readOnly: true }));
});

NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.timeout("60 seconds"), Effect.catchCause(() => Effect.sync(() => {
  console.error(JSON.stringify({ scenario: "deployment-acceptance", status: "failed", error: "Acceptance failed; private diagnostics withheld" }));
  process.exitCode = 1;
})), Effect.provide(Layer.mergeAll(NodeServices.layer, Mcp.layer))));
