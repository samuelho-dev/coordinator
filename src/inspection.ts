import { Effect, Option, Path as PlatformPath, Schema, SchemaParser } from "effect";

export const INSPECTION_LIMIT = 48 * 1024;

const Id = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const Label = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(512));
const Path = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096));
const PrivatePath = Path.pipe(Schema.middlewareDecoding<typeof Path, PlatformPath.Path>((decoded) => Effect.gen(function*() {
  const value = yield* decoded;
  if (Option.isNone(value)) return value;
  const path = yield* PlatformPath.Path;
  return Option.some(yield* SchemaParser.decodeUnknownEffect(Path.check(Schema.makeFilter((input) =>
    path.isAbsolute(input) && path.resolve(input) === input && !/[\0\r\n]/.test(input) ? undefined : "Canonical absolute path without NUL, CR or LF required",
  )))(value.value));
})));
const Nonnegative = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const Count = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const Timestamp = Nonnegative;
const Percent = Schema.Finite;
const bounded = <S extends Schema.Top>(schema: S, maximum: number) => Schema.Array(schema).check(Schema.isMaxLength(maximum));

export const Harness = Schema.Literals(["pi", "omp", "hermes", "codex", "claude"]);
export type Harness = typeof Harness.Type;
export const SessionRef = Schema.Struct({
  machineId: Id, harness: Harness, nativeSessionId: Id, nativeAgentId: Schema.optional(Id),
});
export type SessionRef = typeof SessionRef.Type;
export const sessionRefKey = (ref: SessionRef): string => JSON.stringify([
  ref.machineId, ref.harness, ref.nativeSessionId, ref.nativeAgentId ?? null,
]);

export const ProcessIdentity = Schema.Struct({
  pid: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 0x7fffffff })),
  startIdentity: Id,
  ownership: Schema.Literals(["owned", "shared", "unknown"]),
});
export type ProcessIdentity = typeof ProcessIdentity.Type;
const NativeWorkState = Schema.Struct({ id: Id, state: Schema.Literals(["pending", "active", "completed", "cancelled", "unknown"]) });
const NativeReference = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096), Schema.makeFilter((value) =>
  /[\u0000-\u001f\u007f-\u009f]/u.test(value) || !value.trim() ? "Nonblank text without control characters required" : undefined,
));
export const ProgramStatus = Schema.Union([
  Schema.Struct({ state: Schema.Literals(["idle", "working", "done", "error"]) }),
  Schema.Struct({ state: Schema.Literal("blocked"), kind: Schema.Literals(["permission", "question", "auth"]) }),
]);
export type ProgramStatus = typeof ProgramStatus.Type;

export const MetricUnit = Schema.Literals(["count", "tokens", "bytes", "cores", "milliseconds", "percent", "percent-one-core", "usd", "premium-requests", "provider-defined"]);
export type MetricUnit = typeof MetricUnit.Type;
export const MetricProvenance = Schema.Struct({
  source: Label,
  scope: Schema.Literals(["host", "cgroup", "process", "owned-processes", "current-context", "latest-native-update", "native-aggregate", "sample-window", "unknown"]),
  scopeDescription: Label,
  observedAt: Schema.NullOr(Timestamp), nativeUpdatedAt: Schema.NullOr(Timestamp),
  estimated: Schema.Boolean, derived: Schema.Boolean, shared: Schema.Boolean, partial: Schema.Boolean, stale: Schema.Boolean,
  sampleWindow: Schema.optional(Schema.Struct({ startedAt: Timestamp, endedAt: Timestamp }).check(
    Schema.makeFilter((window) => window.endedAt >= window.startedAt ? undefined : "Sample window must end after its start"),
  )),
});
export type MetricProvenance = typeof MetricProvenance.Type;
const metricSchema = <U extends MetricUnit, S extends Schema.Constraint>(unit: U, value: S) => Schema.Union([
  Schema.Struct({ status: Schema.Literal("available"), value, unit: Schema.Literal(unit), provenance: MetricProvenance }),
  Schema.Struct({ status: Schema.Literal("unavailable"), value: Schema.Null, reason: Label, unit: Schema.Literal(unit), provenance: MetricProvenance }),
]);
export const Metric = Schema.Union([
  metricSchema("count", Count), metricSchema("tokens", Count), metricSchema("bytes", Count),
  metricSchema("milliseconds", Nonnegative), metricSchema("percent", Percent), metricSchema("percent-one-core", Nonnegative),
  metricSchema("cores", Nonnegative),
  metricSchema("usd", Nonnegative), metricSchema("premium-requests", Nonnegative), metricSchema("provider-defined", Nonnegative),
]);
export type Metric = typeof Metric.Type;
export const metric = <U extends MetricUnit>(unit: U, value: number, provenance: MetricProvenance): {
  readonly status: "available"; readonly value: number; readonly unit: U; readonly provenance: MetricProvenance;
} => ({ status: "available", value, unit, provenance });
export const unavailableMetric = <U extends MetricUnit>(unit: U, reason = "Not provided by the native session", provenance: MetricProvenance = {
  source: "unavailable", scope: "unknown", scopeDescription: "No native measurement available",
  observedAt: null, nativeUpdatedAt: null, estimated: false, derived: false, shared: false, partial: false, stale: false,
}): { readonly status: "unavailable"; readonly value: null; readonly reason: string; readonly unit: U; readonly provenance: MetricProvenance } => ({
  status: "unavailable", value: null, reason, unit, provenance,
});
export const MachineResources = Schema.Struct({
  scope: Schema.Literals(["host", "cgroup", "unknown"]), scopeId: Schema.NullOr(Id), source: Label, observedAt: Timestamp,
  cpu: Schema.Struct({
    usagePercent: metricSchema("percent-one-core", Nonnegative),
    capacityCores: metricSchema("cores", Nonnegative), usedPercent: metricSchema("percent", Nonnegative),
  }),
  memory: Schema.Struct({
    usedBytes: metricSchema("bytes", Count), capacityBytes: metricSchema("bytes", Count),
    availableBytes: metricSchema("bytes", Count), usedPercent: metricSchema("percent", Nonnegative),
  }),
});
export type MachineResources = typeof MachineResources.Type;
export const staleMachineResources = (resources: MachineResources): MachineResources => {
  const stale = <M extends Metric>(value: M): M => ({ ...value, provenance: { ...value.provenance, stale: true } });
  return { ...resources,
    cpu: { usagePercent: stale(resources.cpu.usagePercent), capacityCores: stale(resources.cpu.capacityCores), usedPercent: stale(resources.cpu.usedPercent) },
    memory: { usedBytes: stale(resources.memory.usedBytes), capacityBytes: stale(resources.memory.capacityBytes), availableBytes: stale(resources.memory.availableBytes), usedPercent: stale(resources.memory.usedPercent) },
  };
};
export const SessionResources = Schema.Struct({
  observedAt: Timestamp, machineScopeId: Schema.NullOr(Id), attribution: Schema.Literals(["owned", "shared", "unknown"]),
  cpuPercent: metricSchema("percent-one-core", Nonnegative), cpuCapacityPercent: metricSchema("percent", Nonnegative),
  rssBytes: metricSchema("bytes", Count), rssCapacityPercent: metricSchema("percent", Nonnegative),
});
export type SessionResources = typeof SessionResources.Type;
export const staleSessionResources = (resources: SessionResources): SessionResources => {
  const stale = <M extends Metric>(value: M): M => ({ ...value, provenance: { ...value.provenance, stale: true } });
  return { ...resources, cpuPercent: stale(resources.cpuPercent), cpuCapacityPercent: stale(resources.cpuCapacityPercent),
    rssBytes: stale(resources.rssBytes), rssCapacityPercent: stale(resources.rssCapacityPercent) };
};
export const SessionMachine = Schema.Struct({
  name: Schema.optional(Label),
  resources: Schema.optional(MachineResources),
  os: Schema.optional(Schema.Struct({ platform: Schema.Literals(["linux", "darwin"]), release: Schema.optional(Id), architecture: Schema.optional(Id) })),
});
export type SessionMachine = typeof SessionMachine.Type;
export const SessionAgent = Schema.Struct({
  kind: Schema.optional(Id), name: Schema.optional(Id), profile: Schema.optional(Id),
});
export type SessionAgent = typeof SessionAgent.Type;
export const SessionModel = Schema.Struct({ provider: Schema.optional(Id), id: Schema.optional(Id) });
export type SessionModel = typeof SessionModel.Type;
export const SessionProject = Schema.Struct({
  root: Path, source: Schema.Literals(["native", "configured"]), id: Schema.optional(Id), name: Schema.optional(Label),
});
export type SessionProject = typeof SessionProject.Type;
export const SessionGit = Schema.Union([
  Schema.Struct({
    state: Schema.Literal("available"), observedAt: Timestamp, worktreeRoot: Path, commonDir: Path,
    branch: Schema.NullOr(Id), head: Schema.NullOr(Schema.String.check(Schema.isPattern(/^(?:[a-f0-9]{40}|[a-f0-9]{64})(?![\s\S])/))),
  }),
  Schema.Struct({ state: Schema.Literal("not-repository"), observedAt: Timestamp }),
  Schema.Struct({ state: Schema.Literal("unavailable"), observedAt: Timestamp }),
]);
export type SessionGit = typeof SessionGit.Type;
export const SessionWorkspace = Schema.Struct({
  cwd: Path, git: Schema.optional(SessionGit), project: Schema.optional(SessionProject),
});
export type SessionWorkspace = typeof SessionWorkspace.Type;
export const SessionTerminal = Schema.Struct({ name: NativeReference, version: Schema.optional(NativeReference) });
export type SessionTerminal = typeof SessionTerminal.Type;
export const SessionPresence = Schema.Struct({
  ref: SessionRef,
  observedAt: Timestamp,
  harnessVersion: Schema.optional(Id),
  machine: Schema.optional(SessionMachine), agent: Schema.optional(SessionAgent),
  model: Schema.optional(SessionModel), workspace: Schema.optional(SessionWorkspace),
  // Source-host observedAt does not determine receiver-clock connection liveness.
  connection: Schema.optional(Schema.Struct({ state: Schema.Literals(["live", "stale"]), receivedAt: Timestamp, liveUntil: Timestamp })),
  nativeUpdatedAt: Schema.optional(Schema.NullOr(Timestamp)),
  startedAt: Schema.optional(Schema.NullOr(Timestamp)),
  lastActivityAt: Schema.optional(Schema.NullOr(Timestamp)),
  parent: Schema.optional(SessionRef),
  label: Schema.optional(Label),
  terminal: Schema.optional(SessionTerminal),
  programStatus: Schema.optional(ProgramStatus),
  processes: Schema.optional(bounded(ProcessIdentity, 128)),
  resources: Schema.optional(SessionResources),
  activity: Schema.optional(Schema.Struct({
    state: Schema.Literals(["idle", "working", "waiting", "stopped", "unknown"]),
    wait: Schema.optional(Schema.Literals(["api", "tool", "user", "approval", "unknown"])),
    approval: Schema.optional(Schema.Literals(["pending", "approved", "denied", "unknown"])),
    streaming: Schema.optional(Schema.Boolean), compacting: Schema.optional(Schema.Boolean),
    currentToolName: Schema.optional(Id),
    goals: Schema.optional(bounded(NativeWorkState, 128)), todos: Schema.optional(bounded(NativeWorkState, 128)),
  })),
});
export type SessionPresence = typeof SessionPresence.Type;

const TokenMetric = metricSchema("tokens", Count);
const CountMetric = metricSchema("count", Count);
const TimeMetric = metricSchema("milliseconds", Nonnegative);
const UsdMetric = metricSchema("usd", Nonnegative);
export const ProcessObservation = Schema.Struct({
  process: ProcessIdentity,
  cpuPercent: metricSchema("percent-one-core", Nonnegative), cpuTimeMs: TimeMetric, rssBytes: metricSchema("bytes", Count),
});
export type ProcessObservation = typeof ProcessObservation.Type;
export const ResourceObservation = Schema.Struct({
  processes: bounded(ProcessObservation, 128),
  ownedProcessCount: CountMetric, ownedCpuPercent: metricSchema("percent-one-core", Nonnegative), ownedRssBytes: metricSchema("bytes", Count),
});
export type ResourceObservation = typeof ResourceObservation.Type;
export const ContextObservation = Schema.Struct({
  contextWindowTokens: TokenMetric, contextUsedTokens: TokenMetric, contextRemainingTokens: TokenMetric,
  contextUsedPercent: metricSchema("percent", Percent), contextRemainingPercent: metricSchema("percent", Percent),
  systemPromptTokens: TokenMetric, systemToolsTokens: TokenMetric, systemContextTokens: TokenMetric, skillsTokens: TokenMetric, messagesTokens: TokenMetric,
});
export type ContextObservation = typeof ContextObservation.Type;
export const TokenObservation = Schema.Struct({
  inputTokens: TokenMetric, outputTokens: TokenMetric, cacheReadTokens: TokenMetric, cacheWriteTokens: TokenMetric, reasoningOutputTokens: TokenMetric, totalTokens: TokenMetric,
});
export type TokenObservation = typeof TokenObservation.Type;
export const CostObservation = Schema.Struct({
  totalCostUsd: UsdMetric, inputCostUsd: UsdMetric, outputCostUsd: UsdMetric, cacheReadCostUsd: UsdMetric, cacheWriteCostUsd: UsdMetric,
  // Provider-defined counters retain the provider's native meaning in source and scopeDescription.
  premiumRequests: metricSchema("premium-requests", Nonnegative), creditCost: metricSchema("provider-defined", Nonnegative), committedCreditCost: metricSchema("provider-defined", Nonnegative), acuCost: metricSchema("provider-defined", Nonnegative),
});
export type CostObservation = typeof CostObservation.Type;
export const ActivityObservation = Schema.Struct({
  userMessageCount: CountMetric, assistantMessageCount: CountMetric, toolCallCount: CountMetric, toolResultCount: CountMetric,
  totalMessageCount: CountMetric, pendingMessageCount: CountMetric, pendingAsyncWorkCount: CountMetric, activeAsyncJobCount: CountMetric,
  routedModelCounts: Schema.Union([
    Schema.Struct({ status: Schema.Literal("available"), values: bounded(Schema.Struct({ provider: Schema.optional(Id), model: Id, count: CountMetric }), 128), provenance: MetricProvenance }),
    Schema.Struct({ status: Schema.Literal("unavailable"), values: Schema.Null, reason: Label, provenance: MetricProvenance }),
  ]),
  sessionRunningMs: TimeMetric, apiWaitMs: TimeMetric,
});
export type ActivityObservation = typeof ActivityObservation.Type;
export const SessionInspection = Schema.Struct({
  presence: SessionPresence, queriedAt: Timestamp,
  resources: ResourceObservation, context: ContextObservation,
  tokens: Schema.Struct({ latestNativeUpdate: TokenObservation, nativeAggregate: TokenObservation }),
  cost: Schema.Struct({ latestNativeUpdate: CostObservation, nativeAggregate: CostObservation }),
  activity: ActivityObservation,
});
export type SessionInspection = typeof SessionInspection.Type;
const emptyTokens = (): TokenObservation => ({
  inputTokens: unavailableMetric("tokens"), outputTokens: unavailableMetric("tokens"), cacheReadTokens: unavailableMetric("tokens"),
  cacheWriteTokens: unavailableMetric("tokens"), reasoningOutputTokens: unavailableMetric("tokens"), totalTokens: unavailableMetric("tokens"),
});
const emptyCost = (): CostObservation => ({
  totalCostUsd: unavailableMetric("usd"), inputCostUsd: unavailableMetric("usd"), outputCostUsd: unavailableMetric("usd"),
  cacheReadCostUsd: unavailableMetric("usd"), cacheWriteCostUsd: unavailableMetric("usd"), premiumRequests: unavailableMetric("premium-requests"),
  creditCost: unavailableMetric("provider-defined"), committedCreditCost: unavailableMetric("provider-defined"), acuCost: unavailableMetric("provider-defined"),
});
export const createInspection = (presence: SessionPresence, queriedAt: number): SessionInspection => ({
  presence, queriedAt,
  resources: { processes: [], ownedProcessCount: unavailableMetric("count"), ownedCpuPercent: unavailableMetric("percent-one-core"), ownedRssBytes: unavailableMetric("bytes") },
  context: {
    contextWindowTokens: unavailableMetric("tokens"), contextUsedTokens: unavailableMetric("tokens"), contextRemainingTokens: unavailableMetric("tokens"),
    contextUsedPercent: unavailableMetric("percent"), contextRemainingPercent: unavailableMetric("percent"),
    systemPromptTokens: unavailableMetric("tokens"), systemToolsTokens: unavailableMetric("tokens"), systemContextTokens: unavailableMetric("tokens"), skillsTokens: unavailableMetric("tokens"), messagesTokens: unavailableMetric("tokens"),
  },
  tokens: { latestNativeUpdate: emptyTokens(), nativeAggregate: emptyTokens() },
  cost: { latestNativeUpdate: emptyCost(), nativeAggregate: emptyCost() },
  activity: {
    userMessageCount: unavailableMetric("count"), assistantMessageCount: unavailableMetric("count"), toolCallCount: unavailableMetric("count"), toolResultCount: unavailableMetric("count"),
    totalMessageCount: unavailableMetric("count"), pendingMessageCount: unavailableMetric("count"), pendingAsyncWorkCount: unavailableMetric("count"), activeAsyncJobCount: unavailableMetric("count"),
    routedModelCounts: { status: "unavailable", values: null, reason: "Not provided by the native session", provenance: unavailableMetric("count").provenance },
    sessionRunningMs: unavailableMetric("milliseconds"), apiWaitMs: unavailableMetric("milliseconds"),
  },
});

const InspectorUrl = Path.check(Schema.makeFilter((value) => {
  if (!URL.canParse(value)) return "Valid inspector URL required";
  const url = new URL(value);
  if (url.username || url.password || url.search || url.hash) return "Inspector URL must not contain credentials, query or fragment";
  if (url.protocol === "https:") return undefined;
  return url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname) ? undefined : "TLS required except on loopback";
}));
export const InspectorPrincipal = Schema.Struct({
  id: Id, owner: Id, tokenHash: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
});
export type InspectorPrincipal = typeof InspectorPrincipal.Type;
export const InspectorConfig = Schema.Struct({
  host: Id, port: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 65535 })), principals: bounded(InspectorPrincipal, 256),
});
export type InspectorConfig = typeof InspectorConfig.Type;
export const ConnectorConfig = Schema.Struct({
  inspectorUrl: InspectorUrl,
  apiToken: Schema.optional(Schema.String.check(Schema.isMinLength(16), Schema.isMaxLength(4096), Schema.isPattern(/^[A-Za-z0-9._~+\/-]+=*$/))),
  tokenFile: Schema.optional(PrivatePath),
  project: Schema.optional(Schema.Struct({ root: PrivatePath, id: Schema.optional(Id), name: Schema.optional(Label) })),
});
export type ConnectorConfig = typeof ConnectorConfig.Type;
export class HttpError extends Schema.TaggedError<HttpError>()("HttpError", {
  status: Schema.Int.check(Schema.isBetween({ minimum: 400, maximum: 599 })), message: Label,
}) {}
export const ObservationError = Schema.Struct({
  code: Schema.Literals(["unavailable", "not-found", "timeout", "disconnected", "invalid-observation"]),
  message: Schema.Literal("Native session observation unavailable"),
});
export type ObservationError = typeof ObservationError.Type;
const ConnectorIdentity = { connectorId: Id, incarnation: Id };
export const ConnectorAnnounce = Schema.Struct({ ...ConnectorIdentity, machineId: Id, sessions: bounded(SessionPresence, 256) });
export type ConnectorAnnounce = typeof ConnectorAnnounce.Type;
export const ConnectorAccepted = Schema.Struct({ accepted: Schema.Literal(true) });
export type ConnectorAccepted = typeof ConnectorAccepted.Type;
export const ConnectorPoll = Schema.Struct(ConnectorIdentity);
export type ConnectorPoll = typeof ConnectorPoll.Type;
export const InspectionRequest = Schema.Struct({ requestId: Id, ref: SessionRef });
export type InspectionRequest = typeof InspectionRequest.Type;
export const ConnectorPollResult = Schema.Struct({ requests: bounded(InspectionRequest, 32) });
export type ConnectorPollResult = typeof ConnectorPollResult.Type;
export const ConnectorResult = Schema.Struct({
  ...ConnectorIdentity, requestId: Id, observation: Schema.Union([SessionInspection, ObservationError]),
});
export type ConnectorResult = typeof ConnectorResult.Type;
export const ConnectorDisconnect = Schema.Struct(ConnectorIdentity);
export type ConnectorDisconnect = typeof ConnectorDisconnect.Type;
export const SessionListInput = Schema.Struct({
  cursor: Schema.optional(Id), limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
  machineId: Schema.optional(Id), harness: Schema.optional(Harness),
});
export type SessionListInput = typeof SessionListInput.Type;
export const SessionListOutput = Schema.Struct({ sessions: bounded(SessionPresence, 100), nextCursor: Schema.NullOr(Id) });
export type SessionListOutput = typeof SessionListOutput.Type;
export const SessionInspectInput = Schema.Struct({ ref: SessionRef });
export type SessionInspectInput = typeof SessionInspectInput.Type;
export const SessionInspectOutput = SessionInspection;
export type SessionInspectOutput = SessionInspection;
// Boundary decoders reject excess fields recursively instead of silently stripping private content.
export const decodeSessionPresence = Schema.decodeUnknownEffect(SessionPresence, { onExcessProperty: "error" });
export const decodeSessionInspection = Schema.decodeUnknownEffect(SessionInspection, { onExcessProperty: "error" });
export const decodeInspectorConfig = Schema.decodeUnknownEffect(InspectorConfig, { onExcessProperty: "error" });
export const decodeConnectorConfig = Schema.decodeUnknownEffect(ConnectorConfig, { onExcessProperty: "error" });
export const decodeConnectorAnnounce = Schema.decodeUnknownEffect(ConnectorAnnounce, { onExcessProperty: "error" });
export const decodeConnectorPoll = Schema.decodeUnknownEffect(ConnectorPoll, { onExcessProperty: "error" });
export const decodeConnectorResult = Schema.decodeUnknownEffect(ConnectorResult, { onExcessProperty: "error" });
export const decodeConnectorDisconnect = Schema.decodeUnknownEffect(ConnectorDisconnect, { onExcessProperty: "error" });
export const decodeConnectorPollResult = Schema.decodeUnknownEffect(ConnectorPollResult, { onExcessProperty: "error" });
export const decodeSessionListInput = Schema.decodeUnknownEffect(SessionListInput, { onExcessProperty: "error" });
export const decodeSessionListOutput = Schema.decodeUnknownEffect(SessionListOutput, { onExcessProperty: "error" });
export const decodeSessionInspectInput = Schema.decodeUnknownEffect(SessionInspectInput, { onExcessProperty: "error" });
export const InspectionTools = {
  list: { name: "session.list", description: "List same-owner native session presence, without session contents.", input: SessionListInput, output: SessionListOutput },
  inspect: { name: "session.inspect", description: "Read native session counters and attributable process resources on demand, without session contents.", input: SessionInspectInput, output: SessionInspectOutput },
};
