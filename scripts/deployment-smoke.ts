import { parseArgs } from "node:util";
import { ByteSize, ConfigProvider, Context, Effect, Formatter, Layer, Schema, Stdio, Stream } from "effect";
import { HttpClient, HttpClientRequest, HttpIncomingMessage } from "effect/http";
import { NodeRuntime, NodeStdio } from "@effect/platform-node";
import { Commands, CommandError, ConnectHosts, HttpError, PluginConfig, Repo } from "../src/contracts.ts";
import { Evidence, RunnerTask, SessionId } from "../src/generated/database/types.ts";
import { Mcp } from "../src/mcp.ts";
import { Platform, redactedCause } from "../src/platform.ts";

const help = `Usage: node --import tsx scripts/deployment-smoke.ts --config <private PluginConfig> --source-sha <full40hex> [--cycles <1-100>]

Authenticated, no-dispatch deployment acceptance. Cycles default to 5.
Requires a private, schema-valid PluginConfig and private bearer token file for
an existing principal with the session role. No worker enrollment is performed.
HTTPS certificate/hostname verification remains enabled; connectHosts changes
socket DNS only, not the TLS authority/SNI. HTTP is allowed only for disposable
loopback fixtures (including any connectHosts override).
Up to 5 cycles concurrently renew/poll a unique session on a unique .invalid
repository without a workerId. No task, model, real repository or runner is invoked.
Checks missing/invalid credentials, readiness status/body/source SHA, MCP health,
and DB-backed isolated polling with observed concurrency and request latencies.
Always attempts scoped session.stop on failure, including a lost heartbeat response.
All calls together have a 60-second deadline; cleanup has a separate 10 seconds.
One bounded JSON evidence record is written to stdout; failure exits nonzero.
No credentials, config paths, session identifiers or response payloads are logged.
`;
const fail = (message: string) => Effect.fail(new CommandError({ message }));
const emit = (value: string) => Stdio.Stdio.use((stdio) => Stream.make(value).pipe(Stream.run(stdio.stdout())));

const program = Effect.gen(function*() {
  const flags = yield* Effect.try({
    try: () => parseArgs({ options: { config: { type: "string" }, "source-sha": { type: "string" }, cycles: { type: "string" }, help: { type: "boolean" } }, strict: true, allowPositionals: false }),
    catch: () => new CommandError({ message: "Invalid arguments" }),
  });
  if (flags.values.help) { yield* emit(help); return; }
  const configPath = flags.values.config;
  const sourceSha = flags.values["source-sha"];
  const cyclesText = flags.values.cycles ?? "5";
  // ponytail: cap acceptance at 100 cycles; sustained throughput belongs in a load-test harness.
  if (!configPath || !sourceSha || !/^[1-9][0-9]*$/.test(cyclesText) || !Number.isSafeInteger(Number(cyclesText)) || Number(cyclesText) > 100) return yield* fail("Invalid arguments");
  const expectedSha = yield* Schema.decodeUnknownEffect(Evidence.fields.sha)(sourceSha);
  const cycles = Number(cyclesText);
  const platform = yield* Platform;
  const config = yield* Schema.decodeEffect(Schema.fromJsonString(PluginConfig), { onExcessProperty: "error" })(yield* platform.privateFile(configPath));
  if (!platform.path.isAbsolute(configPath) || !platform.path.isAbsolute(config.tokenFile) || !platform.path.isAbsolute(config.stateDirectory)) return yield* fail("Absolute private paths required");
  const token = yield* platform.privateFile(config.tokenFile);
  if (!token || token.length > 505 || /[\r\n]/.test(token)) return yield* fail("Invalid credential file");
  const endpoint = yield* Effect.try({ try: () => new URL(config.brokerUrl), catch: () => new CommandError({ message: "Invalid endpoint" }) });
  if (endpoint.username || endpoint.password || endpoint.search || endpoint.hash || endpoint.pathname !== "/") return yield* fail("Broker authority required");
  const hosts = config.connectHosts === undefined
    ? yield* Schema.decodeEffect(Schema.fromJsonString(ConnectHosts))(process.env.COORDINATOR_CONNECT_HOSTS_JSON ?? "{}")
    : config.connectHosts;
  if (endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]", "::1"].includes(endpoint.hostname) && ["127.0.0.1", "localhost", "[::1]", "::1"].includes(hosts[endpoint.hostname] ?? endpoint.hostname))) return yield* fail("TLS required except loopback fixtures");
  const context = yield* Layer.build(Mcp.layer.pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({
    COORDINATOR_CONNECT_HOSTS_JSON: yield* Schema.encodeEffect(Schema.fromJsonString(ConnectHosts))(hosts),
  })))));
  const mcp = Context.get(context, Mcp);
  const http = Context.get(context, HttpClient.HttpClient);
  const invalidToken = `deployment-invalid-${yield* platform.crypto.randomUUIDv4()}`;
  const sessionId = SessionId.make(`deployment-smoke-${yield* platform.crypto.randomUUIDv4()}`);
  const incarnation = RunnerTask.fields.incarnation.make(yield* platform.crypto.randomUUIDv4());
  // .invalid cannot identify a routable repository; no workerId means no dispatch candidate.
  const repo = Repo.make(`deployment-smoke.invalid/no-dispatch/${yield* platform.crypto.randomUUIDv4()}`);
  const key = { sessionId, incarnation };
  let attempted = false;
  let stopped = false;
  const concurrency = Math.min(cycles, 5);
  let probeElapsedMs = 0;
  let inFlight = 0;
  let peakInFlight = 0;
  let completedWhileConcurrent = 0;
  const latencies = {
    heartbeat: { count: 0, minMs: Infinity, maxMs: 0, totalMs: 0 },
    poll: { count: 0, minMs: Infinity, maxMs: 0, totalMs: 0 },
  };
  const measured = <A, E, R>(kind: keyof typeof latencies, call: Effect.Effect<A, E, R>) => Effect.suspend(() => {
    const started = performance.now();
    inFlight++;
    peakInFlight = Math.max(peakInFlight, inFlight);
    return call.pipe(
      Effect.tap(() => Effect.sync(() => {
        const elapsed = performance.now() - started;
        const sample = latencies[kind];
        sample.count++;
        sample.minMs = Math.min(sample.minMs, elapsed);
        sample.maxMs = Math.max(sample.maxMs, elapsed);
        sample.totalMs += elapsed;
        if (inFlight > 1) completedWhileConcurrent++;
      })),
      Effect.ensuring(Effect.sync(() => { inFlight--; })),
    );
  });
  const ready = (credential?: string) => Effect.scoped(Effect.gen(function*() {
    let request = HttpClientRequest.get(new URL("/readyz", endpoint));
    if (credential !== undefined) request = HttpClientRequest.setHeader(request, "authorization", `Bearer ${credential}`);
    const response = yield* http.execute(request);
    const text = yield* response.text.pipe(Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(1024)));
    return { status: response.status, sha: response.headers["x-coordinator-source-sha"], text };
  })).pipe(Effect.timeout(10000));
  const rejectMcp = (credential: string) => mcp.call(config.brokerUrl, credential, Commands.health, {}).pipe(Effect.matchEffect({
    onSuccess: () => fail("Unauthenticated MCP accepted"),
    onFailure: (error) => error instanceof HttpError && (error.status === 401 || error.status === 403) ? Effect.void : fail("MCP authentication rejection not proven"),
  }));
  yield* Effect.scoped(Effect.gen(function*() {
    yield* Effect.addFinalizer(() => attempted ? mcp.call(config.brokerUrl, token, Commands.stopSession, key, 10000).pipe(
      Effect.flatMap((result) => result.stopped === true ? Effect.sync(() => { stopped = true; }) : fail("Session cleanup not proven")),
      Effect.orDie,
    ) : Effect.void);
    yield* Effect.gen(function*() {
      for (const credential of [undefined, invalidToken]) {
        const response = yield* ready(credential);
        if (response.status !== 401 && response.status !== 403) return yield* fail("Readiness authentication rejection not proven");
      }
      yield* rejectMcp("");
      yield* rejectMcp(invalidToken);
      const response = yield* ready(token);
      if (response.status !== 200 || response.sha !== expectedSha) return yield* fail("Release readiness mismatch");
      const body = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ status: Schema.Literal("ok") })), { onExcessProperty: "error" })(response.text);
      if (body.status !== "ok" || (yield* mcp.call(config.brokerUrl, token, Commands.health, {})).status !== "ok") return yield* fail("Authenticated health mismatch");
      const started = performance.now();
      function* cycleIndexes() { for (let cycle = 0; cycle < cycles; cycle++) yield cycle; }
      yield* Effect.forEach(cycleIndexes(), () => Effect.gen(function*() {
        attempted = true; // Arm before the write: a lost response may still have enrolled the session.
        const heartbeat = yield* measured("heartbeat", mcp.call(config.brokerUrl, token, Commands.heartbeatSession, { ...key, machineId: config.machineId, host: config.host, repo }));
        if (!Number.isFinite(heartbeat.liveUntil) || Object.keys(heartbeat).some((name) => name !== "liveUntil")) return yield* fail("Heartbeat isolation mismatch");
        const poll = yield* measured("poll", mcp.call(config.brokerUrl, token, Commands.pollSession, key));
        if (poll.messages.length || poll.offers.length || poll.releases.length || poll.archiveEnabled) return yield* fail("Session isolation mismatch");
      }), { concurrency, discard: true });
      probeElapsedMs = performance.now() - started;
      if (latencies.heartbeat.count !== cycles || latencies.poll.count !== cycles || (cycles > 1 && (peakInFlight < 2 || completedWhileConcurrent === 0))) return yield* fail("Concurrent request progress not proven");
    }).pipe(Effect.timeout(60000));
  }));
  if (!stopped) return yield* fail("Session cleanup not proven");
  yield* emit(`${Formatter.formatJson({ scenario: "deployment-acceptance", status: "ok", sourceSha: expectedSha, cycles, authRejected: true, readiness: true, mcpHealth: true, databasePoll: true, noDispatch: true, emptyDeliveries: true, sessionStopped: true, probe: { elapsedMs: probeElapsedMs, concurrencyLimit: concurrency, peakInFlight, completedWhileConcurrent, latencies } }, { space: 2 })}\n`);
});

NodeRuntime.runMain(program.pipe(
  Effect.scoped,
  Effect.catchCause((cause) => emit(`${Formatter.formatJson({ scenario: "deployment-acceptance", status: "failed", error: redactedCause(cause).slice(0, 512) }, { space: 2 })}\n`).pipe(Effect.andThen(Effect.sync(() => { process.exitCode = 1; })))),
  Effect.provide(Layer.mergeAll(Platform.layer, NodeStdio.layer)),
));
