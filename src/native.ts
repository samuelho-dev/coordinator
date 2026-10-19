import { randomUUID, createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import { constants } from "node:fs";
import { homedir } from "node:os";
import * as path from "node:path";
import { Console, Data, Effect, Schema } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { Stream } from "effect";
import { Mcp } from "./mcp.ts";
import { INSPECTION_LIMIT, ConnectorConfig, ConnectorAnnounce, ConnectorAccepted, ConnectorPoll, ConnectorPollResult, ConnectorResult, ConnectorDisconnect, SessionInspection, createInspection, metric, unavailableMetric, sessionRefKey, type ObservationError, type SessionPresence, type SessionRef, type MetricProvenance, type MetricUnit } from "./inspection.ts";
import { identifyProcess, observeResources, type ProcessIdentification } from "./resources.ts";

export class NativeError extends Data.TaggedError("NativeError")<{ readonly operation: string }> {}
const io = <A>(operation: string, run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: () => new NativeError({ operation }) });
const json = (text: string) => Effect.try({ try: (): unknown => JSON.parse(text), catch: () => new NativeError({ operation: "Invalid native JSON" }) });
const decode = <S extends Schema.Top>(schema: S, value: unknown) => Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => new NativeError({ operation: "Invalid native metadata" })));
const maximumBytes = 65536;
const cacheLifetime = 120000;

// Open without following a final symlink, validate the opened inode, and bound the read.
export const readPrivateFile = (file: string) => io("Read private file", async () => {
  if (!path.isAbsolute(file) || path.resolve(file) !== file) throw new Error("path");
  const parent = path.dirname(file);
  const directory = await fs.lstat(parent);
  if (!directory.isDirectory() || directory.isSymbolicLink() || (directory.mode & 0o077) !== 0 || directory.uid !== process.getuid?.() || await fs.realpath(parent) !== parent) throw new Error("directory");
  const handle = await fs.open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const info = await handle.stat();
    if (!info.isFile() || info.uid !== process.getuid?.() || (info.mode & 0o077) !== 0 || info.size > maximumBytes) throw new Error("file");
    const bytes = Buffer.alloc(maximumBytes + 1);
    const result = await handle.read(bytes, 0, bytes.length, 0);
    if (result.bytesRead > maximumBytes) throw new Error("bound");
    return bytes.toString("utf8", 0, result.bytesRead);
  } finally { await handle.close(); }
});
export const loadConnectorConfig = (file?: string) => Effect.gen(function*() {
  const explicit = file ?? process.env.COORDINATOR_CONNECTOR_CONFIG;
  const target = explicit ?? path.join(process.env.XDG_CONFIG_HOME ?? path.join(homedir(), ".config"), "coordinator", "inspector.json");
  if (explicit === undefined) {
    const exists = yield* io("Locate connector configuration", () => fs.lstat(target).then(() => true, (error: unknown) => {
      const code = Schema.decodeUnknownOption(Schema.Struct({ code: Schema.Literal("ENOENT") }))(error);
      if (code._tag === "Some") return false;
      throw error;
    }));
    if (!exists) return undefined;
  }
  return yield* Schema.decodeUnknownEffect(ConnectorConfig, { onExcessProperty: "error" })(yield* json(yield* readPrivateFile(target))).pipe(Effect.mapError(() => new NativeError({ operation: "Invalid connector configuration" })));
});
export const readConnectorToken = (config: ConnectorConfig) => Effect.gen(function*() {
  const token = (yield* readPrivateFile(config.tokenFile)).trim();
  if (token.length < 16 || token.length > 4096 || /\s/.test(token)) return yield* new NativeError({ operation: "Invalid connector credential" });
  return token;
});

export const withProcessResources = (inspection: SessionInspection) => Effect.gen(function*() {
  const roots = inspection.presence.processes ?? [];
  if (roots.length === 0) {
    if (Buffer.byteLength(JSON.stringify(inspection)) > INSPECTION_LIMIT - 2 * 1024) return yield* new NativeError({ operation: "Native inspection exceeds connector budget" });
    return inspection;
  }
  const sample = yield* observeResources(roots.map((root) => ({ pid: root.pid, startTime: root.startIdentity, ownership: root.ownership === "owned" ? "exclusive" : root.ownership })));
  const provenance = (scope: "process" | "owned-processes", shared: boolean, item?: typeof sample.processes[number]): MetricProvenance => ({
    source: "native-os", scope,
    scopeDescription: scope === "process" ? `Identified OS process; 100 percent is one core; RSS is not unique physical RAM; CPU monotonic sample interval ${item?.windowMs ?? "unknown"} milliseconds` : "Unique exclusively attributable process tree; overall sampling span; RSS sum is not unique physical RAM",
    observedAt: item ? item.sampledAt : sample.sampledAt, nativeUpdatedAt: null,
    estimated: false, derived: true, shared, partial: sample.partial || sample.truncated || (item !== undefined && item.unavailableReason !== null), stale: false,
    ...(item ? item.sampleStartedAt !== null && item.sampledAt !== null ? { sampleWindow: { startedAt: item.sampleStartedAt, endedAt: item.sampledAt } } : {} : { sampleWindow: { startedAt: sample.sampleStartedAt, endedAt: sample.sampledAt } }),
  });
  const measured = <U extends MetricUnit>(unit: U, value: number | null, p: MetricProvenance) => value === null ? unavailableMetric(unit, sample.unavailableReason ?? "OS measurement unavailable", p) : metric(unit, value, p);
  const shared = sample.sharedProcessCount > 0 || sample.unknownProcessCount > 0;
  const rows: Array<typeof inspection.resources.processes[number]> = [];
  const resources = {
    processes: rows,
    ownedProcessCount: measured("count", sample.partial || sample.truncated || shared ? null : sample.ownedProcessCount, provenance("owned-processes", shared)),
    ownedCpuPercent: measured("percent-one-core", shared ? null : sample.ownedCpuPercent, provenance("owned-processes", shared)),
    ownedRssBytes: measured("bytes", shared ? null : sample.ownedRssBytes, provenance("owned-processes", shared)),
  };
  // Leave normalization headroom inside the registry's observation limit.
  const result = { ...inspection, resources };
  let bytes = Buffer.byteLength(JSON.stringify(result));
  let truncated = false;
  for (const item of sample.processes) {
    if (item.startTime === null) continue;
    const p = provenance("process", item.ownership !== "exclusive", item);
    const row: typeof inspection.resources.processes[number] = { process: { pid: item.pid, startIdentity: item.startTime, ownership: item.ownership === "exclusive" ? "owned" : item.ownership }, cpuPercent: measured("percent-one-core", item.cpuPercent, p), cpuTimeMs: measured("milliseconds", item.cpuTimeMs, p), rssBytes: measured("bytes", item.rssBytes, p) };
    const size = Buffer.byteLength(JSON.stringify(row)) + (rows.length > 0 ? 1 : 0);
    if (bytes + size > INSPECTION_LIMIT - 2 * 1024 || rows.length >= 128) {
      truncated = true;
      break;
    }
    bytes += size;
    rows.push(row);
  }
  if (truncated) {
    const partial = <T extends { readonly provenance: MetricProvenance }>(value: T) => ({ ...value, provenance: { ...value.provenance, partial: true } });
    result.resources = { ...resources,
      processes: resources.processes.map((row) => ({ ...row, cpuPercent: partial(row.cpuPercent), cpuTimeMs: partial(row.cpuTimeMs), rssBytes: partial(row.rssBytes) })),
      ownedProcessCount: partial(resources.ownedProcessCount), ownedCpuPercent: partial(resources.ownedCpuPercent), ownedRssBytes: partial(resources.ownedRssBytes),
    };
  }
  if (Buffer.byteLength(JSON.stringify(result)) > INSPECTION_LIMIT - 2 * 1024) return yield* new NativeError({ operation: "Native inspection exceeds connector budget" });
  return result;
});

export const connectNative = <E extends { readonly _tag: string }, R>(config: ConnectorConfig, callbacks: {
  readonly presence: () => Effect.Effect<ReadonlyArray<SessionPresence>, E, R>;
  readonly inspect: (ref: SessionRef) => Effect.Effect<SessionInspection, E, R>;
}) => Effect.gen(function*() {
  const mcp = yield* Mcp;
  const token = yield* readConnectorToken(config);
  const identity = { connectorId: randomUUID(), incarnation: randomUUID() };
  const unavailable: ObservationError = { code: "unavailable", message: "Native session observation unavailable" };
  const post = <I extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(route: string, input: I, output: O, value: I["Type"]) => mcp.post(config.inspectorUrl, token, route, input, output, value);
  yield* Effect.addFinalizer(() => post("/connector/disconnect", ConnectorDisconnect, ConnectorAccepted, identity).pipe(Effect.catch((error) => Effect.logWarning("Native inspector disconnect failed", error._tag))));
  const announce = Effect.gen(function*() {
    const sessions = yield* callbacks.presence();
    if (sessions.length > 256) return yield* new NativeError({ operation: "Native presence exceeds bound" });
    yield* post("/connector/announce", ConnectorAnnounce, ConnectorAccepted, { ...identity, machineId: config.machineId, sessions });
  });
  yield* announce.pipe(Effect.catch((error) => Effect.logWarning("Initial native presence unavailable", error._tag)));
  yield* Effect.forkScoped(Effect.forever(Effect.sleep("10 seconds").pipe(Effect.andThen(announce), Effect.catch((error) => Effect.logWarning("Native presence unavailable", error._tag)))));
  const cycle = Effect.gen(function*() {
      const pending = yield* post("/connector/poll", ConnectorPoll, ConnectorPollResult, identity);
      for (const request of pending.requests) {
        const observation = yield* callbacks.inspect(request.ref).pipe(Effect.timeout("8 seconds"), Effect.catch((error) => Effect.logWarning("Native detail getter failed", error._tag).pipe(Effect.as(unavailable))));
        yield* post("/connector/result", ConnectorResult, ConnectorAccepted, { ...identity, requestId: request.requestId, observation });
      }
      yield* Effect.sleep("1 second");
  });
  // Read-only discovery may reconnect; native work is never owned or cancelled here.
  return yield* Effect.forever(cycle.pipe(Effect.catch((error) => Effect.logWarning("Native inspector connection unavailable", error._tag).pipe(Effect.andThen(Effect.sleep("2 seconds"))))));
});

const Id = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const N = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const optionalN = Schema.optional(Schema.NullOr(N));
const Count = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const optionalCount = Schema.optional(Schema.NullOr(Count));
const Hook = Schema.Struct({ session_id: Id, hook_event_name: Id, source: Schema.optional(Id), cwd: Schema.optional(Schema.String.check(Schema.isMaxLength(4096))), model: Schema.optional(Id), tool_name: Schema.optional(Id), to_model: Schema.optional(Id) });
const Usage = Schema.Struct({ input_tokens: optionalCount, output_tokens: optionalCount, cache_creation_input_tokens: optionalCount, cache_read_input_tokens: optionalCount });
const ClaudeStatus = Schema.Struct({ session_id: Id, model: Schema.optional(Schema.Struct({ id: Schema.optional(Id) })), workspace: Schema.optional(Schema.Struct({ current_dir: Schema.optional(Schema.String.check(Schema.isMaxLength(4096))) })), context_window: Schema.optional(Schema.Struct({ current_usage: Schema.optional(Schema.NullOr(Usage)), total_input_tokens: optionalCount, total_output_tokens: optionalCount, context_window_size: optionalCount, used_percentage: optionalN, remaining_percentage: Schema.optional(Schema.NullOr(Schema.Finite)) })), cost: Schema.optional(Schema.Struct({ total_cost_usd: optionalN, total_duration_ms: optionalN, total_api_duration_ms: optionalN })) });
const cachedProvenance = (source: string, scope: MetricProvenance["scope"], at: number, description: string, estimated = false): MetricProvenance => ({ source, scope, scopeDescription: description, observedAt: at, nativeUpdatedAt: null, estimated, derived: false, shared: false, partial: false, stale: false });
const numberMetric = <U extends MetricUnit>(unit: U, value: number | null | undefined, p: MetricProvenance) => value === undefined || value === null ? unavailableMetric(unit, "Native counter not supplied", p) : metric(unit, value, p);
export const projectHook = (harness: "claude" | "codex", machineId: string, value: unknown, at = Date.now()) => Effect.gen(function*() {
  const event = yield* decode(Hook, value);
  const name = event.hook_event_name;
  const activity: NonNullable<SessionPresence["activity"]> = name === "PreCompact" || event.source === "compact" ? { state: "working", compacting: true } : name === "SessionEnd" ? { state: "stopped" } : name === "PermissionRequest" ? { state: "waiting", wait: "approval", approval: "pending", ...(event.tool_name ? { currentToolName: event.tool_name } : {}) } : name === "PreToolUse" ? { state: "working", wait: "tool", ...(event.tool_name ? { currentToolName: event.tool_name } : {}) } : name === "Stop" || name === "Interrupt" ? { state: "idle" } : name === "UserPromptSubmit" ? { state: "working", wait: "api" } : { state: "unknown" };
  const model = event.to_model ?? event.model;
  const presence: SessionPresence = { ref: { machineId, harness, nativeSessionId: event.session_id }, observedAt: at, nativeUpdatedAt: at, lastActivityAt: at, activity, ...(event.cwd ? { cwd: event.cwd } : {}), ...(model ? { model } : {}) };
  return createInspection(presence, at);
});
export const projectClaudeStatus = (machineId: string, value: unknown, at = Date.now()) => Effect.gen(function*() {
  const data = yield* decode(ClaudeStatus, value);
  const inspection = createInspection({ ref: { machineId, harness: "claude", nativeSessionId: data.session_id }, observedAt: at, ...(data.model?.id ? { model: data.model.id } : {}), ...(data.workspace?.current_dir ? { cwd: data.workspace.current_dir } : {}) }, at);
  const p = cachedProvenance("claude-statusline", "latest-native-update", at, "Cached current-context/latest-response figures; not lifetime totals; native counter update time unknown");
  const cp: MetricProvenance = { ...p, scope: "current-context", scopeDescription: "Native input-only context basis including cache read and creation; output excluded" };
  const current = data.context_window?.current_usage;
  const used = current?.input_tokens != null && current.cache_read_input_tokens != null && current.cache_creation_input_tokens != null ? current.input_tokens + current.cache_read_input_tokens + current.cache_creation_input_tokens : null;
  const window = data.context_window?.context_window_size;
  return { ...inspection,
    tokens: { ...inspection.tokens, latestNativeUpdate: { ...inspection.tokens.latestNativeUpdate, inputTokens: numberMetric("tokens", data.context_window?.total_input_tokens, p), outputTokens: numberMetric("tokens", data.context_window?.total_output_tokens, p), cacheReadTokens: numberMetric("tokens", current?.cache_read_input_tokens, p), cacheWriteTokens: numberMetric("tokens", current?.cache_creation_input_tokens, p) } },
    context: { ...inspection.context, contextWindowTokens: numberMetric("tokens", window, cp), contextUsedTokens: numberMetric("tokens", used, { ...cp, derived: true }), contextRemainingTokens: numberMetric("tokens", window != null && used !== null ? Math.max(0, window - used) : null, { ...cp, derived: true }), contextUsedPercent: numberMetric("percent", data.context_window?.used_percentage, cp), contextRemainingPercent: numberMetric("percent", data.context_window?.remaining_percentage, cp) },
    cost: { ...inspection.cost, nativeAggregate: { ...inspection.cost.nativeAggregate, totalCostUsd: numberMetric("usd", data.cost?.total_cost_usd, cachedProvenance("claude-statusline", "native-aggregate", at, "Native client-side session estimate, not billed spend", true)) } },
    activity: { ...inspection.activity, sessionRunningMs: numberMetric("milliseconds", data.cost?.total_duration_ms, cachedProvenance("claude-statusline", "native-aggregate", at, "Native accumulated running duration across resumes, excluding not-running time")), apiWaitMs: numberMetric("milliseconds", data.cost?.total_api_duration_ms, cachedProvenance("claude-statusline", "native-aggregate", at, "Native accumulated API duration")) },
  };
});
const TokenBreakdown = Schema.Struct({ inputTokens: Count, cachedInputTokens: Count, cacheWriteInputTokens: optionalCount, outputTokens: Count, reasoningOutputTokens: Count, totalTokens: Count });
const CodexNotification = Schema.Struct({ version: Schema.Literal(2), source: Schema.Literal("attached-app-server"), method: Schema.Literal("thread/tokenUsage/updated"), params: Schema.Struct({ threadId: Id, turnId: Id, tokenUsage: Schema.Struct({ last: TokenBreakdown, total: TokenBreakdown, modelContextWindow: Schema.NullOr(Count) }) }) });
export const projectCodexNotification = (machineId: string, value: unknown, at = Date.now()) => Effect.gen(function*() {
  const envelope = yield* decode(CodexNotification, value);
  const data = envelope.params;
  const inspection = createInspection({ ref: { machineId, harness: "codex", nativeSessionId: data.threadId }, observedAt: at }, at);
  const tokens = (value: typeof TokenBreakdown.Type, scope: "latest-native-update" | "native-aggregate") => {
    const p = cachedProvenance("codex-app-server-v2", scope, at, scope === "native-aggregate" ? "Native attached-thread total; cache is subset of input, reasoning subset of output" : "Native attached-thread last token update; cache is subset of input, reasoning subset of output");
    return { inputTokens: numberMetric("tokens", value.inputTokens, p), outputTokens: numberMetric("tokens", value.outputTokens, p), cacheReadTokens: numberMetric("tokens", value.cachedInputTokens, p), cacheWriteTokens: numberMetric("tokens", value.cacheWriteInputTokens, p), reasoningOutputTokens: numberMetric("tokens", value.reasoningOutputTokens, p), totalTokens: numberMetric("tokens", value.totalTokens, p) };
  };
  return { ...inspection, tokens: { latestNativeUpdate: tokens(data.tokenUsage.last, "latest-native-update"), nativeAggregate: tokens(data.tokenUsage.total, "native-aggregate") }, context: { ...inspection.context, contextWindowTokens: numberMetric("tokens", data.tokenUsage.modelContextWindow, cachedProvenance("codex-app-server-v2", "current-context", at, "Native attached-thread model context window; no input-only used basis supplied")) } };
});

const HermesUsage = Schema.Struct({ input_tokens: optionalCount, prompt_tokens: optionalCount, output_tokens: optionalCount, completion_tokens: optionalCount, total_tokens: optionalCount, cache_read_tokens: optionalCount, cache_write_tokens: optionalCount, reasoning_tokens: optionalCount });
const HermesHook = Schema.Struct({
  hook_event_name: Schema.Literals(["on_session_start", "on_session_end", "on_session_finalize", "post_api_request"]),
  session_id: Schema.String.check(Schema.isMaxLength(256)),
  cwd: Schema.optional(Schema.String.check(Schema.isMaxLength(4096))), profile: Schema.optional(Id),
  extra: Schema.Struct({ old_session_id: Schema.optional(Id), model: Schema.optional(Id), provider: Schema.optional(Id), usage: Schema.optional(Schema.NullOr(HermesUsage)) }),
});
const Cache = Schema.Struct({ version: Schema.Literal(2), kind: Schema.Literal("inspection"), expiresAt: N, inspection: SessionInspection });
const identifyHermesHost = (pid: number) => identifyProcess(pid).pipe(Effect.catchTag("ResourceError", (error) =>
  Console.error(JSON.stringify({ error: "Hermes hook host identity unavailable", code: error._tag })).pipe(Effect.as<ProcessIdentification>({ status: "unavailable", reason: "host-identity-unavailable" }))));
const cacheDirectory = (config: ConnectorConfig) => path.join(config.stateDirectory, "native-observations");
const cacheFile = (config: ConnectorConfig, ref: SessionRef) => path.join(cacheDirectory(config), (ref.harness === "hermes" ? "hermes-" : "") + createHash("sha256").update(sessionRefKey(ref)).digest("hex") + ".json");
const prepareCache = (config: ConnectorConfig) => io("Prepare private metadata cache", async () => {
  const directory = cacheDirectory(config);
  await fs.mkdir(directory, { recursive: true, mode: 0o700 });
  const info = await fs.lstat(directory);
  if (!info.isDirectory() || (info.mode & 0o077) !== 0 || info.uid !== process.getuid?.() || await fs.realpath(directory) !== directory) throw new Error("directory");
});
const cacheRecords = (config: ConnectorConfig, hermesIdentities = new Map<number, ProcessIdentification>()) => Effect.gen(function*() {
  yield* prepareCache(config);
  const entries = yield* io("List metadata cache", () => fs.readdir(cacheDirectory(config)));
  const records = new Map<string, typeof Cache.Type>();
  for (const entry of entries) {
    if (!/^(hermes-)?[a-f0-9]{64}\.json$/.test(entry)) continue;
    const file = path.join(cacheDirectory(config), entry);
    const parsed = yield* readPrivateFile(file).pipe(Effect.flatMap(json), Effect.map(Schema.decodeUnknownOption(Cache)), Effect.catch((error) => Console.error(JSON.stringify({ error: "Ignoring unsupported native cache", code: error._tag })).pipe(Effect.as(undefined))));
    if (parsed === undefined || parsed._tag === "None") continue;
    const record = parsed.value;
    const ref = record.inspection.presence.ref;
    if (ref.machineId !== config.machineId || cacheFile(config, ref) !== file) continue;
    let liveHermesHost = false;
    if (ref.harness === "hermes" && record.inspection.presence.activity?.state !== "stopped") {
      for (const binding of record.inspection.presence.processes ?? []) {
        let identity = hermesIdentities.get(binding.pid);
        if (identity === undefined) {
          identity = yield* identifyHermesHost(binding.pid);
          hermesIdentities.set(binding.pid, identity);
        }
        if (binding.ownership === "shared" && identity.status === "available" && identity.startTime === binding.startIdentity) liveHermesHost = true;
      }
    }
    if ((!liveHermesHost && record.expiresAt <= Date.now()) || record.inspection.presence.activity?.state === "stopped") {
      yield* io("Expire owned native observation", () => fs.unlink(file));
    } else records.set(file, record);
  }
  if (records.size > 1024) return yield* new NativeError({ operation: "Metadata cache exceeds bound" });
  return records;
});
export const writeNativeCache = (config: ConnectorConfig, incoming: SessionInspection) => Effect.gen(function*() {
  const records = yield* cacheRecords(config);
  const target = cacheFile(config, incoming.presence.ref);
  if (records.size >= 1024 && !records.has(target)) return yield* new NativeError({ operation: "Metadata cache exceeds bound" });
  let inspection = incoming;
  const previous = records.get(target);
  if (previous !== undefined) {
    const old = previous.inspection;
    const metadataOnly = incoming.presence.nativeUpdatedAt !== undefined;
    inspection = { ...(metadataOnly ? old : incoming), presence: { ...old.presence, ...incoming.presence }, queriedAt: incoming.queriedAt, context: metadataOnly && !incoming.presence.activity?.compacting ? old.context : incoming.context };
  }
  yield* io("Write private metadata cache", async () => {
    // An unsupported file, even inside our namespace, is not ours to replace.
    if (!records.has(target)) {
      try { await fs.lstat(target); throw new Error("unsupported cache input"); }
      catch (error) { if (Schema.decodeUnknownOption(Schema.Struct({ code: Schema.Literal("ENOENT") }))(error)._tag !== "Some") throw error; }
    }
    const text = JSON.stringify({ version: 2, kind: "inspection", expiresAt: incoming.presence.observedAt + cacheLifetime, inspection });
    if (Buffer.byteLength(text) > maximumBytes) throw new Error("bound");
    const temporary = target + "." + randomUUID();
    await fs.writeFile(temporary, text, { flag: "wx", mode: 0o600 });
    try { await fs.rename(temporary, target); } finally { await fs.rm(temporary, { force: true }); }
  });
});
export const writeHermesHook = (config: ConnectorConfig, value: unknown, at = Date.now(), hostPid = process.ppid) => Effect.gen(function*() {
  const event = yield* decode(HermesHook, value);
  const sessionId = event.hook_event_name === "on_session_finalize" ? event.extra.old_session_id ?? event.session_id : event.session_id;
  if (!sessionId) return yield* new NativeError({ operation: "Missing native session identity" });
  const ref: SessionRef = { machineId: config.machineId, harness: "hermes", nativeSessionId: sessionId };
  const previous = (yield* cacheRecords(config)).get(cacheFile(config, ref))?.inspection;
  if (!previous && event.hook_event_name !== "on_session_start") return;
  // Hermes shell_hooks.py directly Popen's argv (shell=False); /usr/bin/env execs Node.
  // Only this direct-exec contract attributes PPID, as a shared hook host, never a session-owned process.
  const identity = yield* identifyHermesHost(hostPid);
  const binding = previous?.presence.processes?.at(0);
  if (binding && (identity.status !== "available" || binding.pid !== hostPid || binding.startIdentity !== identity.startTime)) return;
  const presence: SessionPresence = {
    ref, observedAt: at, lastActivityAt: at,
    ...(event.hook_event_name !== "post_api_request" ? { nativeUpdatedAt: at } : {}),
    activity: { state: event.hook_event_name === "on_session_finalize" ? "stopped" : event.hook_event_name === "on_session_end" ? "idle" : "unknown" },
    ...(event.cwd ? { cwd: event.cwd } : {}), ...(event.profile ? { profile: event.profile } : {}),
    ...(event.extra.model ? { model: event.extra.model } : {}), ...(event.extra.provider ? { provider: event.extra.provider } : {}),
    ...(!previous && identity.status === "available" ? { processes: [{ pid: hostPid, startIdentity: identity.startTime, ownership: "shared" }] } : {}),
  };
  const inspection = createInspection(presence, at);
  const p = cachedProvenance("hermes-post_api_request", "latest-native-update", at, "Cached successful native provider-request usage, not session aggregate; native update time unknown");
  const usage = event.extra.usage;
  yield* writeNativeCache(config, event.hook_event_name !== "post_api_request" ? inspection : { ...inspection, tokens: { ...inspection.tokens, latestNativeUpdate: {
    inputTokens: numberMetric("tokens", usage?.input_tokens ?? usage?.prompt_tokens, p), outputTokens: numberMetric("tokens", usage?.output_tokens ?? usage?.completion_tokens, p),
    cacheReadTokens: numberMetric("tokens", usage?.cache_read_tokens, p), cacheWriteTokens: numberMetric("tokens", usage?.cache_write_tokens, p),
    reasoningOutputTokens: numberMetric("tokens", usage?.reasoning_tokens, p), totalTokens: numberMetric("tokens", usage?.total_tokens, p),
  } } });
});
export const readNativeCache = (config: ConnectorConfig) => Effect.gen(function*() {
  const observations: SessionInspection[] = [];
  const hermesIdentities = new Map<number, ProcessIdentification>();
  for (const record of (yield* cacheRecords(config, hermesIdentities)).values()) {
    const inspection = record.inspection;
    const alive = [];
    for (const binding of inspection.presence.processes ?? []) {
      const identity = inspection.presence.ref.harness === "hermes" ? hermesIdentities.get(binding.pid) : yield* identifyProcess(binding.pid);
      if (identity?.status === "available" && identity.startTime === binding.startIdentity) alive.push(binding);
    }
    if ((inspection.presence.processes?.length ?? 0) > 0 && alive.length === 0) continue;
    observations.push({ ...inspection, presence: { ...inspection.presence, ...(inspection.presence.ref.harness === "hermes" && alive.length > 0 ? { observedAt: Date.now() } : {}), processes: alive } });
  }
  return observations;
});

const ClaudeRoster = Schema.Array(Schema.Struct({ sessionId: Schema.optional(Id), id: Schema.optional(Id), pid: Schema.optional(Schema.Int), cwd: Schema.optional(Schema.String), kind: Schema.optional(Id), status: Schema.optional(Schema.Literals(["busy", "waiting", "idle"])), waitingFor: Schema.optional(Schema.String) })).check(Schema.isMaxLength(256));
export const claudeRoster = (config: ConnectorConfig) => Effect.scoped(Effect.gen(function*() {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner.spawn(ChildProcess.make("claude", ["agents", "--json"], { stdin: "ignore" }));
  yield* Effect.forkScoped(Stream.runDrain(child.stderr));
  let bytes = 0;
  const text = yield* child.stdout.pipe(Stream.mapEffect((chunk) => {
    bytes += chunk.length;
    return bytes > maximumBytes ? Effect.fail(new NativeError({ operation: "Native roster exceeds bound" })) : Effect.succeed(chunk);
  }), Stream.decodeText(), Stream.runFold(() => "", (all, chunk) => all + chunk));
  if ((yield* child.exitCode) !== 0) return yield* new NativeError({ operation: "Native Claude roster unavailable" });
  const roster = yield* decode(ClaudeRoster, yield* json(text));
  const sessions: SessionPresence[] = [];
  for (const row of roster) {
    const id = row.sessionId ?? row.id;
    if (!id || !row.pid) continue;
    const identity = yield* identifyProcess(row.pid);
    if (identity.status !== "available") continue;
    sessions.push({ ref: { machineId: config.machineId, harness: "claude", nativeSessionId: id }, observedAt: Date.now(), ...(row.cwd ? { cwd: row.cwd } : {}), ...(row.kind ? { kind: row.kind } : {}), processes: [{ pid: row.pid, startIdentity: identity.startTime, ownership: "owned" }], activity: { state: row.status === "busy" ? "working" : row.status === "waiting" ? "waiting" : row.status === "idle" ? "idle" : "unknown", ...(row.waitingFor === "permission prompt" ? { wait: "approval", approval: "pending" } : {}) } });
  }
  return sessions;
})).pipe(Effect.timeout("5 seconds"));
export const connectExternal = (config: ConnectorConfig) => {
  let latest = new Map<string, SessionInspection>();
  return connectNative(config, {
    presence: () => Effect.gen(function*() {
      const cached = yield* readNativeCache(config);
      latest = new Map(cached.map((observation) => [sessionRefKey(observation.presence.ref), observation]));
      const roster = yield* claudeRoster(config).pipe(Effect.catch((error) => Effect.logWarning("Claude roster unavailable", error._tag).pipe(Effect.as([]))));
      for (const presence of roster) {
        const old = latest.get(sessionRefKey(presence.ref));
        latest.set(sessionRefKey(presence.ref), { ...(old ?? createInspection(presence, Date.now())), presence: { ...old?.presence, ...presence } });
      }
      return Array.from(latest.values(), (observation) => observation.presence);
    }),
    inspect: (ref) => Effect.gen(function*() {
      const observation = latest.get(sessionRefKey(ref));
      if (!observation) return yield* new NativeError({ operation: "Native session unavailable" });
      return yield* withProcessResources({ ...observation, queriedAt: Date.now() });
    }),
  });
};
