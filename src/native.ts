import { Cause, Clock, Config, Console, Crypto, Data, Duration, Effect, FileSystem, Option, Path, PlatformError, Schedule, Schema, Scope } from "effect";
import { Hex } from "effect/encoding";
import { Host } from "./platform.ts";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { Stream } from "effect";
import { Mcp } from "./mcp.ts";
import { INSPECTION_LIMIT, ConnectorConfig, ConnectorAnnounce, ConnectorAccepted, ConnectorPoll, ConnectorPollResult, ConnectorResult, ConnectorDisconnect, SessionInspection, createInspection, metric, unavailableMetric, sessionRefKey, type ObservationError, type SessionPresence, type SessionRef, type MetricProvenance, type MetricUnit } from "./inspection.ts";
import { identifyProcess, observeResources, type ProcessIdentification } from "./resources.ts";
import { readRexContext } from "./rex.ts";

export class NativeError extends Data.TaggedError("NativeError")<{ readonly operation: string }> {}
export type NativeConfig = ConnectorConfig & { readonly machineId: string; readonly stateDirectory: string };
export type NativeNotice = {
  readonly state: "unavailable" | "recovered" | "error";
  readonly operation: string;
  readonly code?: string | undefined;
  readonly status?: number | undefined;
};
const json = (text: string) => Effect.try({ try: (): unknown => JSON.parse(text), catch: () => new NativeError({ operation: "Invalid native JSON" }) });
const decode = <S extends Schema.Top>(schema: S, value: unknown) => Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => new NativeError({ operation: "Invalid native metadata" })));
const maximumBytes = 65536;
const cacheLifetime = 120000;

const platformFailure = (operation: string) => () => new NativeError({ operation });
// FileSystem finalizers close with orDie; sanitize only their platform defects.
const sanitizeFileFinalizers = (operation: string) => <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(
  Effect.catchCause((cause) => Effect.failCause(Cause.fromReasons<E | NativeError>(cause.reasons.map((reason) =>
    reason._tag === "Die" && PlatformError.isPlatformError(reason.defect) && reason.defect.reason.module === "FileSystem"
      ? Cause.makeFailReason(new NativeError({ operation }))
      : reason)))),
);
const validateDirectory = (directory: string, exact = false) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const host = yield* Host;
  const info = yield* fs.stat(directory);
  if (info.type !== "Directory" || Option.getOrUndefined(info.uid) !== host.uid || host.uid === undefined || (info.mode & (exact ? 0o777 : 0o077)) !== (exact ? 0o700 : 0) || (yield* fs.realPath(directory)) !== directory) {
    return yield* new NativeError({ operation: "Unsafe private directory" });
  }
});
// Validate the opened inode, not a subsequent path lookup; reads may be partial.
const readPrivateText = (file: string, exact = false) => Effect.scoped(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* Host;
  if (!path.isAbsolute(file) || path.resolve(file) !== file) return yield* new NativeError({ operation: "Unsafe private path" });
  yield* validateDirectory(path.dirname(file));
  const handle = yield* fs.open(file, { flag: "r", noFollow: true });
  const info = yield* handle.stat;
  if (info.type !== "File" || host.uid === undefined || Option.getOrUndefined(info.uid) !== host.uid || (info.mode & (exact ? 0o777 : 0o077)) !== (exact ? 0o600 : 0) || info.size > maximumBytes) return yield* new NativeError({ operation: "Unsafe private file" });
  const bytes = new Uint8Array(maximumBytes + 1);
  let length = 0;
  while (length < bytes.length) {
    const count = yield* handle.read(bytes.subarray(length));
    if (count === 0) break;
    length += count;
  }
  if (length > maximumBytes) return yield* new NativeError({ operation: "Private file exceeds bound" });
  return new TextDecoder().decode(bytes.subarray(0, length));
})).pipe(sanitizeFileFinalizers("Read private file"));
export const readPrivateFile = (file: string) => readPrivateText(file).pipe(Effect.mapError(platformFailure("Read private file")));
export const loadConnectorConfig = (file?: string) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* Host;
  const selected = yield* Config.String("COORDINATOR_CONNECTOR_CONFIG").pipe(Config.option);
  const xdg = yield* Config.String("XDG_CONFIG_HOME").pipe(Config.option);
  const explicit = file ?? Option.getOrUndefined(selected);
  const target = explicit ?? path.join(Option.getOrUndefined(xdg) ?? path.join(host.homeDirectory, ".config"), "coordinator", "inspector.json");
  const exists = explicit !== undefined || (yield* fs.readDirectory(path.dirname(target)).pipe(
    Effect.map((entries) => entries.includes(path.basename(target))),
    Effect.catch((error) => error.reason._tag === "NotFound" ? Effect.succeed(false) : Effect.fail(error)),
    Effect.mapError(platformFailure("Locate connector configuration")),
  ));
  const fields = yield* Schema.decodeUnknownEffect(Schema.Struct({
    inspectorUrl: Schema.optional(Schema.Unknown), apiToken: Schema.optional(Schema.Unknown), tokenFile: Schema.optional(Schema.Unknown),
  }), { onExcessProperty: "error" })(exists ? yield* json(yield* readPrivateFile(target)) : {}).pipe(
    Effect.mapError(platformFailure("Invalid connector configuration")),
  );
  const url = yield* Config.String("COORDINATOR_API_URL").pipe(Config.option);
  const token = yield* Config.String("COORDINATOR_API_TOKEN").pipe(Config.option);
  const inspectorUrl = Option.getOrUndefined(url) ?? fields.inspectorUrl;
  if (!exists && inspectorUrl === undefined) return undefined;
  return yield* Schema.decodeUnknownEffect(ConnectorConfig, { onExcessProperty: "error" })({
    ...fields,
    ...(inspectorUrl === undefined ? {} : { inspectorUrl }),
    ...(Option.isNone(token) ? {} : { apiToken: token.value }),
  }).pipe(Effect.mapError(platformFailure("Invalid connector configuration")));
});
export const readConnectorToken = (config: ConnectorConfig) => Effect.gen(function*() {
  if (config.apiToken !== undefined) return config.apiToken;
  if (config.tokenFile === undefined) return yield* new NativeError({ operation: "Connector credential required" });
  const token = (yield* readPrivateFile(config.tokenFile)).trim();
  if (token.length < 16 || token.length > 4096 || !/^[A-Za-z0-9._~+\/-]+=*$/.test(token)) return yield* new NativeError({ operation: "Invalid connector credential" });
  return token;
});

export const resolveNativeConfig = (config: ConnectorConfig) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const host = yield* Host;
  const xdg = yield* Config.String("XDG_STATE_HOME").pipe(Config.option);
  const base = Option.getOrUndefined(xdg) ?? (host.platform === "darwin" ? path.join(host.homeDirectory, "Library", "Application Support") : path.join(host.homeDirectory, ".local", "state"));
  if (!path.isAbsolute(base) || path.resolve(base) !== base || /[\0\r\n]/.test(base)) return yield* new NativeError({ operation: "Unsafe state path" });
  const stateDirectory = path.join(base, "coordinator");
  yield* fs.makeDirectory(stateDirectory, { recursive: true, mode: 0o700 });
  yield* validateDirectory(stateDirectory, true);
  const target = path.join(stateDirectory, "machine-id");
  const readIdentity = readPrivateText(target, true).pipe(Effect.flatMap((text) =>
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\n$/.test(text)
      ? Effect.succeed(text.slice(0, -1)) : Effect.fail(new NativeError({ operation: "Invalid installation identity" }))));
  const existing = yield* readIdentity.pipe(Effect.map(Option.some), Effect.catch((error) =>
    error._tag === "PlatformError" && error.reason._tag === "NotFound" ? Effect.succeed(Option.none<string>()) : Effect.fail(error)));
  if (Option.isSome(existing)) return { ...config, machineId: existing.value, stateDirectory };
  const temporary = path.join(stateDirectory, `.machine-id-${yield* crypto.randomUUIDv4()}`);
  const machineId = yield* crypto.randomUUIDv4();
  return yield* Effect.scoped(Effect.gen(function*() {
    const cleanupScope = yield* Effect.scope;
    yield* Effect.scoped(Effect.uninterruptibleMask((restore) => Effect.gen(function*() {
      const handle = yield* fs.open(temporary, { flag: "wx", mode: 0o600 });
      yield* Scope.addFinalizer(cleanupScope, fs.remove(temporary).pipe(Effect.orDie));
      yield* restore(handle.writeAll(new TextEncoder().encode(machineId + "\n")));
    })));
    yield* fs.link(temporary, target).pipe(Effect.catch((error) =>
      error.reason._tag === "AlreadyExists" ? Effect.void : Effect.fail(error)));
    return yield* readIdentity;
  })).pipe(Effect.map((identity) => ({ ...config, machineId: identity, stateDirectory })));
}).pipe(sanitizeFileFinalizers("Resolve private native installation identity"), Effect.mapError(platformFailure("Resolve private native installation identity")));

export const withProcessResources = (inspection: SessionInspection) => Effect.gen(function*() {
  const roots = inspection.presence.processes ?? [];
  if (roots.length === 0) {
    if (new TextEncoder().encode(JSON.stringify(inspection)).length > INSPECTION_LIMIT - 2 * 1024) return yield* new NativeError({ operation: "Native inspection exceeds connector budget" });
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
  let bytes = new TextEncoder().encode(JSON.stringify(result)).length;
  let truncated = false;
  for (const item of sample.processes) {
    if (item.startTime === null) continue;
    const p = provenance("process", item.ownership !== "exclusive", item);
    const row: typeof inspection.resources.processes[number] = { process: { pid: item.pid, startIdentity: item.startTime, ownership: item.ownership === "exclusive" ? "owned" : item.ownership }, cpuPercent: measured("percent-one-core", item.cpuPercent, p), cpuTimeMs: measured("milliseconds", item.cpuTimeMs, p), rssBytes: measured("bytes", item.rssBytes, p) };
    const size = new TextEncoder().encode(JSON.stringify(row)).length + (rows.length > 0 ? 1 : 0);
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
  if (new TextEncoder().encode(JSON.stringify(result)).length > INSPECTION_LIMIT - 2 * 1024) return yield* new NativeError({ operation: "Native inspection exceeds connector budget" });
  return result;
});

export const connectNative = <E extends { readonly _tag: string }, R>(config: NativeConfig, callbacks: {
  readonly presence: () => Effect.Effect<ReadonlyArray<SessionPresence>, E, R>;
  readonly inspect: (ref: SessionRef) => Effect.Effect<SessionInspection, E, R>;
  readonly report?: ((notice: NativeNotice) => Effect.Effect<void>) | undefined;
}) => Effect.gen(function*() {
  const mcp = yield* Mcp;
  const clock = yield* Clock.Clock;
  const now = () => Number(clock.monotonicTimeNanosUnsafe() / 1_000_000n);
  let token = yield* readConnectorToken(config);
  const crypto = yield* Crypto.Crypto;
  const identity = { connectorId: yield* crypto.randomUUIDv4(), incarnation: yield* crypto.randomUUIDv4() };
  const report = callbacks.report ?? ((notice: NativeNotice) => Effect.logDebug({ message: "Native inspector", state: notice.state, operation: notice.operation, code: notice.code, status: notice.status }));
  let isUnavailable = false;
  let hasReportedHealthy = false;
  let failedAt: number | undefined;
  let healthyAt: number | undefined;
  const failure = (state: "unavailable" | "error", operation: string, error: { readonly _tag: string; readonly status?: number | undefined }): NativeNotice => ({
    state, operation, code: error._tag, status: error.status,
  });
  const unavailable = (operation: string, error: { readonly _tag: string; readonly status?: number | undefined }) => Effect.gen(function*() {
    healthyAt = undefined;
    failedAt ??= now();
    if (isUnavailable || now() - failedAt < 5000) return;
    isUnavailable = true;
    yield* report(failure("unavailable", operation, error));
  });
  const recovered = Effect.gen(function*() {
    healthyAt ??= now();
    if (now() - healthyAt < 10000) return false;
    const resetBackoff = failedAt !== undefined;
    failedAt = undefined;
    if (!isUnavailable && hasReportedHealthy) return resetBackoff;
    const shouldReport = isUnavailable || callbacks.report !== undefined;
    isUnavailable = false;
    hasReportedHealthy = true;
    if (shouldReport) yield* report({ state: "recovered", operation: "connection" });
    return resetBackoff;
  });
  const post = <I extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(route: string, input: I, output: O, value: I["Type"]) =>
    mcp.post(config.inspectorUrl, token, route, input, output, value).pipe(Effect.tapError((error) => unavailable(route, error)));
  // ponytail: disconnect is advisory; registry expiry handles an unreachable receiver.
  yield* Effect.addFinalizer(() => mcp.post(config.inspectorUrl, token, "/connector/disconnect", ConnectorDisconnect, ConnectorAccepted, identity, 500).pipe(
    Effect.catch((error) => Effect.logDebug({ operation: "/connector/disconnect", code: error._tag, status: error._tag === "HttpError" ? error.status : undefined })),
  ));
  const announce = Effect.gen(function*() {
    const sessions = yield* callbacks.presence();
    if (sessions.length > 256) return yield* new NativeError({ operation: "Native presence exceeds bound" });
    yield* post("/connector/announce", ConnectorAnnounce, ConnectorAccepted, { ...identity, machineId: config.machineId, sessions });
  }).pipe(Effect.tapError((error) => unavailable("native presence", error)));
  const observationFailure = (error: { readonly _tag: string; readonly status?: number | undefined }) => Effect.logDebug({ operation: "native detail", code: error._tag, status: error.status }).pipe(
    Effect.as<ObservationError>({
      code: error._tag === "TimeoutError" ? "timeout" : error._tag === "HttpError" && error.status === 404 ? "not-found" : "unavailable",
      message: "Native session observation unavailable",
    }),
  );
  const cycle = Effect.gen(function*() {
    const pending = yield* post("/connector/poll", ConnectorPoll, ConnectorPollResult, identity);
    for (const request of pending.requests) {
      const observation = yield* callbacks.inspect(request.ref).pipe(Effect.timeout("8 seconds"), Effect.catch(observationFailure));
      yield* post("/connector/result", ConnectorResult, ConnectorAccepted, { ...identity, requestId: request.requestId, observation });
    }
    if (yield* recovered) return true;
    yield* Effect.sleep("1 second");
    return false;
  });
  const retryable = (error: { readonly _tag: string; readonly status?: number | undefined }) => Effect.gen(function*() {
    if (error._tag === "TimeoutError") return true;
    if (error._tag !== "HttpError") return false;
    if (error.status === 401 || error.status === 403) {
      const next = yield* readConnectorToken(config).pipe(
        Effect.catch((failure) => unavailable("credential reload", failure).pipe(Effect.as(undefined))),
      );
      if (next === undefined || next === token) return false;
      token = next;
      return true;
    }
    return error.status === 409 || error.status === 429 || (error.status !== undefined && error.status >= 500);
  });
  const maximumDelay = Duration.seconds(30);
  const backoff = Schedule.min([Schedule.exponential("2 seconds"), Schedule.spaced(maximumDelay)]).pipe(
    Schedule.jittered,
    Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.min(duration, maximumDelay))),
  );
  // Stable recovery starts a fresh retry schedule; either loop's failure cancels its peer.
  return yield* Effect.forever(Effect.gen(function*() {
    yield* announce;
    yield* Effect.raceFirst(
      Effect.forever(Effect.sleep("10 seconds").pipe(Effect.andThen(announce))),
      cycle.pipe(Effect.repeat({ until: (resetBackoff) => resetBackoff })),
    );
  }).pipe(Effect.retry({ while: retryable, schedule: backoff })));
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
export const projectHook = (harness: "claude" | "codex", machineId: string, value: unknown, at?: number) => Effect.gen(function*() {
  at ??= yield* Clock.currentTimeMillis;
  const event = yield* decode(Hook, value);
  const name = event.hook_event_name;
  const activity: NonNullable<SessionPresence["activity"]> = name === "PreCompact" || event.source === "compact" ? { state: "working", compacting: true } : name === "SessionEnd" ? { state: "stopped" } : name === "PermissionRequest" ? { state: "waiting", wait: "approval", approval: "pending", ...(event.tool_name ? { currentToolName: event.tool_name } : {}) } : name === "PreToolUse" ? { state: "working", wait: "tool", ...(event.tool_name ? { currentToolName: event.tool_name } : {}) } : name === "Stop" || name === "Interrupt" ? { state: "idle" } : name === "UserPromptSubmit" ? { state: "working", wait: "api" } : { state: "unknown" };
  const model = event.to_model ?? event.model;
  const programStatus: SessionPresence["programStatus"] = name === "PermissionRequest" ? { state: "blocked", kind: "permission" } : harness === "claude" && name === "Elicitation" ? { state: "blocked", kind: "question" } : name === "Interrupt" ? { state: "idle" } : harness === "claude" && name === "StopFailure" ? { state: "error" } : harness === "claude" && name === "Stop" ? { state: "done" } : activity.state === "working" || (harness === "claude" && (name === "PostToolUse" || name === "PostToolUseFailure" || name === "ElicitationResult")) ? { state: "working" } : undefined;
  const presence: SessionPresence = { ref: { machineId, harness, nativeSessionId: event.session_id }, observedAt: at, nativeUpdatedAt: at, lastActivityAt: at, activity, ...(programStatus ? { programStatus } : {}), ...(event.cwd ? { cwd: event.cwd } : {}), ...(model ? { model } : {}) };
  return createInspection(presence, at);
});
export const projectClaudeStatus = (machineId: string, value: unknown, at?: number) => Effect.gen(function*() {
  at ??= yield* Clock.currentTimeMillis;
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
export const projectCodexNotification = (machineId: string, value: unknown, at?: number) => Effect.gen(function*() {
  const observedAt = at ?? (yield* Clock.currentTimeMillis);
  const envelope = yield* decode(CodexNotification, value);
  const data = envelope.params;
  const inspection = createInspection({ ref: { machineId, harness: "codex", nativeSessionId: data.threadId }, observedAt }, observedAt);
  const tokens = (value: typeof TokenBreakdown.Type, scope: "latest-native-update" | "native-aggregate") => {
    const p = cachedProvenance("codex-app-server-v2", scope, observedAt, scope === "native-aggregate" ? "Native attached-thread total; cache is subset of input, reasoning subset of output" : "Native attached-thread last token update; cache is subset of input, reasoning subset of output");
    return { inputTokens: numberMetric("tokens", value.inputTokens, p), outputTokens: numberMetric("tokens", value.outputTokens, p), cacheReadTokens: numberMetric("tokens", value.cachedInputTokens, p), cacheWriteTokens: numberMetric("tokens", value.cacheWriteInputTokens, p), reasoningOutputTokens: numberMetric("tokens", value.reasoningOutputTokens, p), totalTokens: numberMetric("tokens", value.totalTokens, p) };
  };
  return { ...inspection, tokens: { latestNativeUpdate: tokens(data.tokenUsage.last, "latest-native-update"), nativeAggregate: tokens(data.tokenUsage.total, "native-aggregate") }, context: { ...inspection.context, contextWindowTokens: numberMetric("tokens", data.tokenUsage.modelContextWindow, cachedProvenance("codex-app-server-v2", "current-context", observedAt, "Native attached-thread model context window; no input-only used basis supplied")) } };
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
const cacheFile = (config: NativeConfig, ref: SessionRef) => Effect.gen(function*() {
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(sessionRefKey(ref)));
  return path.join(config.stateDirectory, "native-observations", (ref.harness === "hermes" ? "hermes-" : "") + Hex.encode(digest) + ".json");
});
const cacheRecords = (config: NativeConfig, hermesIdentities = new Map<number, ProcessIdentification>()) => Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = path.join(config.stateDirectory, "native-observations");
  yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  yield* validateDirectory(directory);
  const entries = yield* fs.readDirectory(directory);
  const records = new Map<string, typeof Cache.Type>();
  for (const entry of entries) {
    if (!/^(hermes-)?[a-f0-9]{64}\.json$/.test(entry)) continue;
    const file = path.join(directory, entry);
    const parsed = yield* readPrivateFile(file).pipe(Effect.flatMap(json), Effect.map(Schema.decodeUnknownOption(Cache)), Effect.catch((error) => Console.error(JSON.stringify({ error: "Ignoring unsupported native cache", code: error._tag })).pipe(Effect.as(undefined))));
    if (parsed === undefined || parsed._tag === "None") continue;
    const record = parsed.value;
    const ref = record.inspection.presence.ref;
    if (ref.machineId !== config.machineId || (yield* cacheFile(config, ref)) !== file) continue;
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
    if ((!liveHermesHost && record.expiresAt <= (yield* Clock.currentTimeMillis)) || record.inspection.presence.activity?.state === "stopped") {
      yield* fs.remove(file);
    } else records.set(file, record);
  }
  if (records.size > 1024) return yield* new NativeError({ operation: "Metadata cache exceeds bound" });
  return records;
}).pipe(sanitizeFileFinalizers("Read private metadata cache"), Effect.catchTag("PlatformError", () => Effect.fail(new NativeError({ operation: "Read private metadata cache" }))));
export const writeNativeCache = (config: NativeConfig, incoming: SessionInspection) => Effect.gen(function*() {
  const rex = yield* readRexContext;
  if (rex) incoming = { ...incoming, presence: { ...incoming.presence, rex } };
  const records = yield* cacheRecords(config);
  const target = yield* cacheFile(config, incoming.presence.ref);
  if (records.size >= 1024 && !records.has(target)) return yield* new NativeError({ operation: "Metadata cache exceeds bound" });
  let inspection = incoming;
  const previous = records.get(target);
  if (previous !== undefined) {
    const old = previous.inspection;
    const metadataOnly = incoming.presence.nativeUpdatedAt !== undefined;
    const { programStatus: previousStatus, ...oldPresence } = old.presence;
    inspection = { ...(metadataOnly ? old : incoming), presence: { ...oldPresence, ...(!incoming.presence.activity && previousStatus ? { programStatus: previousStatus } : {}), ...incoming.presence }, queriedAt: incoming.queriedAt, context: metadataOnly && !incoming.presence.activity?.compacting ? old.context : incoming.context };
  }
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  // Directory entries include dangling symlinks; unsupported inputs are never replaced.
  if (!records.has(target) && (yield* fs.readDirectory(path.dirname(target))).includes(path.basename(target))) return yield* new NativeError({ operation: "Unsupported cache input" });
  const bytes = new TextEncoder().encode(JSON.stringify({ version: 2, kind: "inspection", expiresAt: incoming.presence.observedAt + cacheLifetime, inspection }));
  if (bytes.length > maximumBytes) return yield* new NativeError({ operation: "Metadata cache exceeds bound" });
  const temporary = target + "." + (yield* crypto.randomUUIDv4());
  yield* Effect.scoped(Effect.gen(function*() {
    const cleanupScope = yield* Effect.scope;
    yield* Effect.scoped(Effect.uninterruptibleMask((restore) => Effect.gen(function*() {
      const handle = yield* fs.open(temporary, { flag: "wx", mode: 0o600 });
      yield* Scope.addFinalizer(cleanupScope, fs.remove(temporary, { force: true }).pipe(Effect.orDie));
      yield* restore(handle.writeAll(bytes));
    })));
    yield* fs.rename(temporary, target);
  }));
}).pipe(sanitizeFileFinalizers("Write private metadata cache"), Effect.catchTag("PlatformError", () => Effect.fail(new NativeError({ operation: "Write private metadata cache" }))));
export const writeHermesHook = (config: NativeConfig, value: unknown, at?: number, hostPid?: number) => Effect.gen(function*() {
  at ??= yield* Clock.currentTimeMillis;
  hostPid ??= (yield* Host).ppid;
  const event = yield* decode(HermesHook, value);
  const sessionId = event.hook_event_name === "on_session_finalize" ? event.extra.old_session_id ?? event.session_id : event.session_id;
  if (!sessionId) return yield* new NativeError({ operation: "Missing native session identity" });
  const ref: SessionRef = { machineId: config.machineId, harness: "hermes", nativeSessionId: sessionId };
  const previous = (yield* cacheRecords(config)).get(yield* cacheFile(config, ref))?.inspection;
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
export const readNativeCache = (config: NativeConfig) => Effect.gen(function*() {
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
    observations.push({ ...inspection, presence: { ...inspection.presence, ...(inspection.presence.ref.harness === "hermes" && alive.length > 0 ? { observedAt: yield* Clock.currentTimeMillis } : {}), processes: alive } });
  }
  return observations;
});

const ClaudeRoster = Schema.Array(Schema.Struct({ sessionId: Schema.optional(Id), id: Schema.optional(Id), pid: Schema.optional(Schema.Int), cwd: Schema.optional(Schema.String), kind: Schema.optional(Id), status: Schema.optional(Schema.Literals(["busy", "waiting", "idle"])), waitingFor: Schema.optional(Schema.String) })).check(Schema.isMaxLength(256));
export const claudeRoster = (config: NativeConfig) => Effect.scoped(Effect.gen(function*() {
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
    const programStatus: SessionPresence["programStatus"] = row.waitingFor === "permission prompt" ? { state: "blocked", kind: "permission" } : row.status === "busy" ? { state: "working" } : row.status === "idle" ? { state: "idle" } : undefined;
    sessions.push({ ref: { machineId: config.machineId, harness: "claude", nativeSessionId: id }, observedAt: yield* Clock.currentTimeMillis, ...(row.cwd ? { cwd: row.cwd } : {}), ...(row.kind ? { kind: row.kind } : {}), ...(programStatus ? { programStatus } : {}), processes: [{ pid: row.pid, startIdentity: identity.startTime, ownership: "owned" }], activity: { state: row.status === "busy" ? "working" : row.status === "waiting" ? "waiting" : row.status === "idle" ? "idle" : "unknown", ...(row.waitingFor === "permission prompt" ? { wait: "approval", approval: "pending" } : {}) } });
  }
  return sessions;
})).pipe(Effect.timeout("5 seconds"));
export const connectExternal = (config: NativeConfig) => {
  let latest = new Map<string, SessionInspection>();
  return connectNative(config, {
    presence: () => Effect.gen(function*() {
      const cached = yield* readNativeCache(config);
      latest = new Map(cached.map((observation) => [sessionRefKey(observation.presence.ref), observation]));
      const roster = yield* claudeRoster(config).pipe(Effect.catch((error) => Effect.logDebug({ operation: "native Claude roster", code: error._tag }).pipe(Effect.as([]))));
      for (const presence of roster) {
        const old = latest.get(sessionRefKey(presence.ref));
        latest.set(sessionRefKey(presence.ref), { ...(old ?? createInspection(presence, yield* Clock.currentTimeMillis)), presence: { ...old?.presence, ...presence } });
      }
      return Array.from(latest.values(), (observation) => observation.presence);
    }),
    inspect: (ref) => Effect.gen(function*() {
      const observation = latest.get(sessionRefKey(ref));
      if (!observation) return yield* new NativeError({ operation: "Native session unavailable" });
      return yield* withProcessResources({ ...observation, queriedAt: yield* Clock.currentTimeMillis });
    }),
  });
};
