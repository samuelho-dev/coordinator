#!/usr/bin/env node
import { Cause, Config, Console, Context, Effect, FileSystem, Layer, Logger, Path, Schema, Stdio, Stream } from "effect";
import { HttpServer } from "effect/http";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Inspector } from "./inspector.ts";
import { Mcp, serve, serveStdio } from "./mcp.ts";
import { Host, cliStdioLayer, serverLayer } from "./platform.ts";
import { InspectorConfig, InspectionTools, SessionInspectInput } from "./inspection.ts";
import { NativeError, connectExternal, loadConnectorConfig, resolveNativeConfig, readConnectorToken, readPrivateFile, projectHook, projectClaudeStatus, projectCodexNotification, writeNativeCache, writeHermesHook } from "./native.ts";
import { runSetup } from "./setup.ts";
import type { SessionPresence } from "./inspection.ts";

const readInput = Effect.gen(function*() {
  const stdio = yield* Stdio.Stdio;
  const bytes = new Uint8Array(65536);
  let length = 0;
  yield* Stream.runForEach(stdio.stdin, (chunk) => {
    if (length + chunk.byteLength > bytes.byteLength) return Effect.fail(new NativeError({ operation: "Invalid bounded JSON input" }));
    return Effect.sync(() => { bytes.set(chunk, length); length += chunk.byteLength; });
  });
  return yield* Effect.try({
    try: (): unknown => JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, length))),
    catch: () => new NativeError({ operation: "Invalid bounded JSON input" }),
  });
});

export const cli = (args: ReadonlyArray<string>) => Effect.scoped(Effect.gen(function*() {
  const mode = args.at(0) ?? "help";
  if (mode === "--version") {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = yield* path.fromFileUrl(new URL(import.meta.resolve("@samuelho-dev/coordinator/package.json")));
    const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ version: Schema.String })))(yield* fs.readFileString(file));
    return yield* Console.log(manifest.version);
  }
  if (mode === "help" || mode === "--help") {
    return yield* Console.log(JSON.stringify({ commands: ["init [--directory PATH] [--owner ID] [--listen-host IP] [--port 8787]", "connect [--endpoint URL] [--token-file PATH] [--config PATH]", "inspector", "connector", "mcp", "hook claude|codex|hermes", "claude-statusline", "codex-notification", "list", "inspect"], inspectorConfiguration: "COORDINATOR_CONFIG: private inspector server configuration", configuration: "COORDINATOR_API_URL / COORDINATOR_API_TOKEN override COORDINATOR_CONNECTOR_CONFIG or XDG_CONFIG_HOME/coordinator/inspector.json; credentials may also use tokenFile", setup: "Run coordinator init --help or coordinator connect --help. Credentials are accepted only by masked prompt, private file, or environment.", statusline: "Optional manual composition only; consumes stdin without producing display output", codexNotification: "Version 2 envelope, source attached-app-server, thread/tokenUsage/updated only; does not observe other TUIs" }));
  }
  if (mode === "init" || mode === "connect") return yield* runSetup(args);
  if (mode === "inspector") {
    const file = yield* Config.String("COORDINATOR_CONFIG").pipe(Config.withDefault(""));
    if (!file) return yield* new NativeError({ operation: "Inspector private configuration required" });
    const config = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(InspectorConfig), { onExcessProperty: "error" })(yield* readPrivateFile(file));
    const inspector = yield* Inspector.make(config);
    const context = yield* Layer.build(serverLayer(config.host, config.port));
    yield* serve(Context.get(context, HttpServer.HttpServer), inspector);
    return yield* Effect.never;
  }
  const config = yield* loadConnectorConfig();
  if (!config) return yield* new NativeError({ operation: "Connector configuration required" });
  if (mode === "hook" || mode === "claude-statusline" || mode === "codex-notification") {
    const native = yield* resolveNativeConfig(config);
    const input = yield* readInput;
    const harness = args.at(1);
    if (mode === "hook" && harness === "hermes") return yield* writeHermesHook(native, input);
    const observation = mode === "claude-statusline" ? yield* projectClaudeStatus(native.machineId, input) : mode === "codex-notification" ? yield* projectCodexNotification(native.machineId, input) : harness === "claude" || harness === "codex" ? yield* projectHook(harness, native.machineId, input) : yield* new NativeError({ operation: "Expected native hook harness" });
    yield* writeNativeCache(native, observation);
    return;
  }
  if (mode === "connector") return yield* connectExternal(yield* resolveNativeConfig(config));
  const token = yield* readConnectorToken(config);
  const mcp = yield* Mcp;
  const reader = {
    list: (input: typeof InspectionTools.list.input.Type) => mcp.call(config.inspectorUrl, token, InspectionTools.list, input),
    inspect: (input: typeof SessionInspectInput.Type) => mcp.call(config.inspectorUrl, token, InspectionTools.inspect, input),
  };
  if (mode === "mcp") return yield* serveStdio(reader);
  if (mode === "list") {
    const sessions: SessionPresence[] = [];
    let cursor: string | undefined;
    do {
      const page = yield* reader.list({ limit: 100, ...(cursor === undefined ? {} : { cursor }) });
      sessions.push(...page.sessions);
      cursor = page.nextCursor ?? undefined;
    } while (cursor !== undefined);
    return yield* Console.log(JSON.stringify({ sessions, nextCursor: null }));
  }
  if (mode === "inspect") {
    const input = yield* Schema.decodeUnknownEffect(SessionInspectInput)(yield* readInput);
    return yield* Console.log(JSON.stringify(yield* reader.inspect(input)));
  }
  return yield* new NativeError({ operation: "Unknown inspector command" });
}));

// Imports are inert for fixture tests and native adapters.
if (import.meta.main) {
  NodeRuntime.runMain(Effect.gen(function*() {
    const args = yield* (yield* Stdio.Stdio).args;
    const hook = args.at(0) === "hook" || args.at(0) === "claude-statusline" || args.at(0) === "codex-notification";
    yield* cli(args).pipe(Effect.catchCause((cause) => {
      if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause);
      return Console.error(JSON.stringify({
        error: "Native inspector operation unavailable",
        reasons: cause.reasons.map((reason) => reason._tag === "Fail"
          ? { kind: reason._tag, code: reason.error._tag,
            ...(reason.error._tag === "HttpError" ? { status: reason.error.status } : {}),
            ...(reason.error._tag === "NativeError" ? { operation: reason.error.operation } : {}) }
          : { kind: reason._tag }),
      })).pipe(Effect.andThen(hook ? Effect.void : Effect.failCause(cause)));
    }));
  }).pipe(
    Effect.provide(Layer.mergeAll(NodeServices.layer, cliStdioLayer, Host.layer, Mcp.layer, Layer.succeed(Logger.LogToStderr, true))),
  ), { disableErrorReporting: true });
}
