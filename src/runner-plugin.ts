import { ByteSize, Config, Effect, Exit, Layer, ManagedRuntime, Option, Schema, Scope } from "effect";
import { HttpIncomingMessage, HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { NodeHttpServer, NodeServices } from "@effect/platform-node";
import { createServer } from "node:http";
import { Buffer } from "node:buffer";
import { AgentError, AgentConfig, AgentEndpoint, AgentHostEnum, AgentTools, AgentPrivateResultBytes, boundedJsonSchema } from "./agent/contracts.ts";
import { agentHttpLayer, agentTool } from "./agent/server.ts";
import { Platform } from "./platform.ts";
import { makeAgentSession } from "./agent-session.ts";
import type { AgentSession as PiSession, SessionEntry as PiSessionEntry } from "@earendil-works/pi-coding-agent";
import type { AgentSession as OmpSession, SessionEntry as OmpSessionEntry } from "@oh-my-pi/pi-coding-agent";
import type { Context, Host } from "./contracts.ts";

const transcriptJson = Effect.fn("Plugin.transcriptJson")(function*(value: PiSessionEntry | OmpSessionEntry | PiSession["messages"][number] | OmpSession["messages"][number], kind: "entry" | "message") {
  const json = yield* Effect.try({ try: () => JSON.stringify(value), catch: () => new AgentError({ code: "agent_failure", message: `Owned transcript ${kind} is not JSON serializable` }) });
  if (json === undefined) return yield* new AgentError({ code: "agent_failure", message: `Owned transcript ${kind} is not JSON serializable` });
  if (Buffer.byteLength(json, "utf8") > AgentPrivateResultBytes) return yield* new AgentError({ code: "agent_failure", message: `Owned transcript ${kind} exceeds the agent contract` });
  return yield* Schema.decodeEffect(Schema.fromJsonString(boundedJsonSchema))(json).pipe(Effect.mapError(() => new AgentError({ code: "agent_failure", message: `Owned transcript ${kind} exceeds the agent contract` })));
});

export const makeAgentSessionServer = Effect.fn("Plugin.agentControl")(function*(config: AgentConfig, initial: Pick<Context, "cwd" | "model">) {
  const platform = yield* Platform;
  if (!platform.path.isAbsolute(config.endpointFile) || !platform.path.isAbsolute(config.tokenFile) ||
    (yield* platform.fs.realPath(config.tokenFile)) !== config.tokenFile ||
    ((yield* platform.fs.stat(platform.path.dirname(config.endpointFile))).mode & 0o077) !== 0) {
    return yield* new AgentError({ code: "invalid_input", message: "Canonical private plugin control paths required" });
  }
  if (yield* platform.fs.exists(config.endpointFile)) return yield* new AgentError({ code: "binding_mismatch", message: "Plugin endpoint is already owned; refusing to replace it" });
  const token = yield* platform.privateFile(config.tokenFile);
  if (token.length < 32 || token.length > 480) return yield* new AgentError({ code: "invalid_input", message: "Invalid private plugin credential" });
  const tokenHash = yield* platform.digest(token);
  const directory = platform.path.join(platform.path.dirname(config.endpointFile), "sessions");
  yield* platform.fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  const owner = yield* makeAgentSession(config.host, initial.cwd, directory, initial.model);
  const { binding, journal, bound, session, manager, operation } = owner;
  const tools = [
    agentTool(AgentTools.prompt, (input) => owner.prompt(input)),
    agentTool(AgentTools.abortAndPrompt, (input) => Effect.gen(function*() { yield* bound(input.binding); yield* owner.abort(); return yield* owner.prompt(input); })),
    agentTool(AgentTools.getState, (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const model = session.model;
      const sessionFile = manager.getSessionFile();
      return { binding, ...(sessionFile === undefined ? {} : { sessionFile }), cwd: initial.cwd,
        model: model === undefined ? null : { id: model.id, name: model.name, api: model.api, provider: model.provider, reasoning: model.reasoning,
          input: model.input, cost: model.cost, contextWindow: model.contextWindow ?? null, maxTokens: model.maxTokens ?? null },
        currentRunId: owner.currentRunId, isStreaming: session.isStreaming || owner.pendingSubmissions > 0, isCompacting: session.isCompacting,
        pendingMessageCount: owner.pendingMessageCount,
        pendingAsyncWorkCount: owner.pendingAsyncWorkCount,
        messageCount: session.messages.length, systemPrompt: session.systemPrompt };
    })),
    agentTool(AgentTools.getMessages, (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const entries = manager.getBranch().filter((entry) => entry.type === "message");
      const previous = input.cursor === undefined ? -1 : entries.findIndex((entry) => entry.id === input.cursor);
      if (input.cursor !== undefined && previous < 0) return yield* new AgentError({ code: "resource_gap", message: "Transcript cursor is no longer on this branch" });
      const page = entries.slice(previous + 1, previous + 1 + (input.limit ?? 128));
      const more = previous + 1 + page.length < entries.length;
      return { binding, messages: yield* Effect.forEach(page, (entry) => transcriptJson(entry.message, "message")), nextCursor: more ? page.at(-1)?.id ?? null : null, more };
    })),
    agentTool(AgentTools.abort, (input) => Effect.gen(function*() { yield* bound(input.binding); return yield* owner.abort(); })),
    agentTool(AgentTools.steer, (input) => Effect.gen(function*() {
      const result = yield* owner.queue(input, "steer");
      return result === undefined ? null : { disposition: result };
    })),
    agentTool(AgentTools.followUp, (input) => Effect.gen(function*() {
      const result = yield* owner.queue(input, "followUp");
      return result === undefined ? null : { disposition: result };
    })),
    agentTool(AgentTools.getEntries, (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const entries = manager.getEntries();
      const previous = input.since === undefined ? -1 : entries.findIndex((entry) => entry.id === input.since);
      if (input.since !== undefined && previous < 0) return yield* new AgentError({ code: "resource_gap", message: "Append-history cursor is unavailable" });
      return { entries: yield* Effect.forEach(entries.slice(previous + 1), (entry) => transcriptJson(entry, "entry")), leafId: manager.getLeafId() };
    })),
    agentTool(AgentTools.bash, (input) => Effect.gen(function*() {
      yield* bound(input.binding);
      const result = yield* operation(() => session.executeBash(input.command, undefined, { excludeFromContext: input.excludeFromContext ?? true }));
      return { binding, exitCode: result.exitCode ?? null, output: result.output, truncated: result.truncated, killed: result.cancelled };
    })),
  ];
  const app = yield* HttpRouter.toHttpEffect(agentHttpLayer({ host: config.host, binding, journal, tools, name: "coordinator-owned-sdk", version: "0.1.0" }));
  const server = yield* NodeHttpServer.make(() => createServer({ requestTimeout: 15000 }), { host: "127.0.0.1", port: 0, gracefulShutdownTimeout: "1 second" });
  yield* server.serve(Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const authorization = request.headers.authorization;
    if (!authorization?.startsWith("Bearer ") || authorization.length > 512 || !(yield* platform.checkToken(authorization.slice(7), tokenHash))) return HttpServerResponse.empty({ status: 401, headers: { "cache-control": "no-store" } });
    return yield* app.pipe(Effect.interruptible, Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(64 * 1024)));
  }));
  const address = server.address;
  if (address._tag !== "InetAddressV4") return yield* new AgentError({ code: "agent_failure", message: "Plugin control requires an IPv4 loopback address" });
  const pendingEndpoint = `${config.endpointFile}.${binding.incarnation}.pending`;
  yield* Effect.acquireRelease(platform.fs.writeFileString(pendingEndpoint, Schema.encodeSync(Schema.fromJsonString(AgentEndpoint))({ url: `http://127.0.0.1:${address.port}/mcp`, binding }), { flag: "wx", mode: 0o600 }), () => platform.fs.remove(pendingEndpoint, { force: true }).pipe(Effect.orDie));
  yield* platform.fs.link(pendingEndpoint, config.endpointFile);
  yield* Effect.addFinalizer(() => platform.fs.remove(config.endpointFile).pipe(Effect.orDie));
  yield* platform.fs.remove(pendingEndpoint);
  return owner;
});

export default async function agentControl(host: Pick<Host, "on">): Promise<void> {
  const layer = Layer.merge(Platform.layer, NodeServices.layer);
  let runtime = ManagedRuntime.make(layer);
  let stopped = false;
  try {
    const path = await runtime.runPromise(Config.option(Config.String("COORDINATOR_AGENT_CONFIG")));
    if (Option.isNone(path)) { await runtime.dispose(); return; }
    const config = await runtime.runPromise(Effect.gen(function*() {
      const platform = yield* Platform;
      if (!platform.path.isAbsolute(path.value)) return yield* new AgentError({ code: "invalid_input", message: "Absolute plugin configuration path required" });
      return yield* Schema.decodeEffect(Schema.fromJsonString(AgentConfig), { onExcessProperty: "error" })(yield* platform.privateFile(path.value)).pipe(Effect.mapError(() => new AgentError({ code: "invalid_input", message: "Invalid private agent plugin configuration" })));
    }));
    let scope: Scope.Closeable | undefined;
    const start = async (ctx: Context): Promise<void> => {
      if (ctx.agent?.kind === "sub") return;
      if (stopped) { runtime = ManagedRuntime.make(layer); stopped = false; }
      if (scope !== undefined) await runtime.runPromise(Scope.close(scope, Exit.void));
      const currentScope = await runtime.runPromise(Scope.make());
      scope = currentScope;
      await runtime.runPromise(makeAgentSessionServer(config, ctx).pipe(Effect.provideService(Scope.Scope, currentScope), Effect.onExit((exit) => exit._tag === "Failure" ? Scope.close(currentScope, exit) : Effect.void)));
    };
    host.on("session_start", (_event, ctx) => start(ctx));
    if (config.host === AgentHostEnum.omp) host.on("session_switch", (_event, ctx) => start(ctx));
    host.on("session_shutdown", async () => {
      if (scope !== undefined) { await runtime.runPromise(Scope.close(scope, Exit.void)); scope = undefined; }
      stopped = true;
      await runtime.dispose();
    });
  } catch { await runtime.dispose(); throw new AgentError({ code: "agent_failure", message: "Owned agent plugin initialization failed; check private configuration" }); }
}
