#!/usr/bin/env node
import { pathToFileURL } from "node:url";
import { realpathSync } from "node:fs";
import { Console, Context, Effect, Layer, Schema } from "effect";
import { HttpServer } from "effect/http";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Inspector } from "./inspector.ts";
import { Mcp, serverLayer } from "./mcp.ts";
import { InspectorConfig, InspectionTools, SessionInspectInput } from "./inspection.ts";
import { NativeError, connectExternal, loadConnectorConfig, readConnectorToken, readPrivateFile, projectHook, projectClaudeStatus, projectCodexNotification, writeNativeCache, writeHermesHook } from "./native.ts";

const readInput = Effect.tryPromise({ try: async (): Promise<unknown> => {
  let bytes = 0;
  let text = "";
  for await (const chunk of process.stdin) {
    bytes += Buffer.byteLength(chunk);
    if (bytes > 65536) throw new Error("bound");
    text += chunk;
  }
  return JSON.parse(text);
}, catch: () => new NativeError({ operation: "Invalid bounded JSON input" }) });

export const cli = (args: ReadonlyArray<string>) => Effect.scoped(Effect.gen(function*() {
  const mode = args.at(0) ?? "help";
  if (mode === "help" || mode === "--help") {
    return yield* Console.log(JSON.stringify({ commands: ["inspector", "connector", "mcp", "hook claude|codex|hermes", "claude-statusline", "codex-notification", "list", "inspect"], inspectorConfiguration: "COORDINATOR_CONFIG: private inspector server configuration", configuration: "COORDINATOR_CONNECTOR_CONFIG or XDG_CONFIG_HOME/coordinator/inspector.json", statusline: "Optional manual composition only; consumes stdin without producing display output", codexNotification: "Version 2 envelope, source attached-app-server, thread/tokenUsage/updated only; does not observe other TUIs" }));
  }
  if (mode === "inspector") {
    const file = process.env.COORDINATOR_CONFIG;
    if (!file) return yield* new NativeError({ operation: "Inspector private configuration required" });
    const config = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(InspectorConfig), { onExcessProperty: "error" })(yield* readPrivateFile(file));
    const inspector = yield* Inspector.make(config);
    const context = yield* Layer.build(serverLayer(config.host, config.port));
    const mcp = yield* Mcp;
    yield* mcp.serve(Context.get(context, HttpServer.HttpServer), inspector);
    return yield* Effect.never;
  }
  const config = yield* loadConnectorConfig();
  if (!config) return yield* new NativeError({ operation: "Connector configuration required" });
  if (mode === "hook" || mode === "claude-statusline" || mode === "codex-notification") {
    const input = yield* readInput;
    const harness = args.at(1);
    if (mode === "hook" && harness === "hermes") return yield* writeHermesHook(config, input);
    const observation = mode === "claude-statusline" ? yield* projectClaudeStatus(config.machineId, input) : mode === "codex-notification" ? yield* projectCodexNotification(config.machineId, input) : harness === "claude" || harness === "codex" ? yield* projectHook(harness, config.machineId, input) : yield* new NativeError({ operation: "Expected native hook harness" });
    yield* writeNativeCache(config, observation);
    return;
  }
  if (mode === "connector") return yield* connectExternal(config);
  const token = yield* readConnectorToken(config);
  const mcp = yield* Mcp;
  const reader = {
    list: (input: typeof InspectionTools.list.input.Type) => mcp.call(config.inspectorUrl, token, InspectionTools.list, input),
    inspect: (input: typeof SessionInspectInput.Type) => mcp.call(config.inspectorUrl, token, InspectionTools.inspect, input),
  };
  if (mode === "mcp") return yield* mcp.serveStdio(reader);
  if (mode === "list") return yield* Console.log(JSON.stringify(yield* reader.list({ limit: 100 })));
  if (mode === "inspect") {
    const input = yield* Schema.decodeUnknownEffect(SessionInspectInput)(yield* readInput);
    return yield* Console.log(JSON.stringify(yield* reader.inspect(input)));
  }
  return yield* new NativeError({ operation: "Unknown inspector command" });
}));

// Imports are inert for fixture tests and native adapters.
if (process.argv.at(1) && import.meta.url === pathToFileURL(realpathSync(process.argv.at(1) ?? "")).href) {
  process.umask(0o077);
  const args = process.argv.slice(2);
  const hook = args.at(0) === "hook" || args.at(0) === "claude-statusline" || args.at(0) === "codex-notification";
  NodeRuntime.runMain(cli(args).pipe(
    Effect.provide(Layer.mergeAll(NodeServices.layer, Mcp.layer)),
    Effect.catchCause((cause) => Console.error(JSON.stringify({
      error: "Native inspector operation unavailable",
      reasons: cause.reasons.map((reason) => reason._tag === "Fail" ? { kind: reason._tag, code: reason.error._tag } : { kind: reason._tag }),
    })).pipe(Effect.andThen(Effect.sync(() => { process.exitCode = hook ? 0 : 1; })))),
  ));
}
