import { Effect, Schema, Tuple } from "effect";
import type { McpRequestContext } from "effect/ai/McpSchema";
import { McpSchema } from "effect/ai";

export const AgentMcpProtocolVersion = "2025-11-25";
export const AgentPrivateChunkBytes = 32 * 1024;
export const AgentMcpWireBytes = 64 * 1024;
export const AgentMcpResultBytes = AgentMcpWireBytes - 1024;
export const AgentPrivateResultBytes = 64 * 1024 * 1024;
const utf8 = new TextEncoder();
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const encodePrettyJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown, { space: 2 }));
export const resultByteLimit = Schema.makeFilter<unknown>((value) =>
  utf8.encode(encodeJson({ content: [{ type: "text", text: encodePrettyJson(value) }], structuredContent: value })).byteLength <= AgentMcpResultBytes ? undefined : "Agent result exceeds encoded byte budget",
  // The wire union has only object branches; preserve its root type for standard MCP discovery.
  { toJsonSchema: () => [{ type: "object" }, true] });

const count = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const text = Schema.String.check(Schema.isMaxLength(16000));
export const AgentSdkOutputText = Schema.String.check(Schema.isMaxLength(AgentPrivateResultBytes));
export const AgentHostEnum = Object.freeze({ pi: "pi", omp: "omp" });
export type AgentHostEnum = typeof AgentHostEnum[keyof typeof AgentHostEnum];
export const AgentHost = Schema.Enum(AgentHostEnum);
export type AgentHost = typeof AgentHost.Type;
export const AgentSessionId = Schema.String.pipe(Schema.brand("AgentSessionId"));
export type AgentSessionId = typeof AgentSessionId.Type;
export const AgentIncarnationId = Schema.String.pipe(Schema.brand("AgentIncarnationId"));
export type AgentIncarnationId = typeof AgentIncarnationId.Type;
export const AgentGeneration = count.pipe(Schema.brand("AgentGeneration"));
export type AgentGeneration = typeof AgentGeneration.Type;
export const AgentRunId = Schema.String.pipe(Schema.brand("AgentRunId"));
export type AgentRunId = typeof AgentRunId.Type;
export const AgentSubmissionId = Schema.String.pipe(Schema.brand("AgentSubmissionId"));
export type AgentSubmissionId = typeof AgentSubmissionId.Type;
export const AgentInputId = Schema.String.pipe(Schema.brand("AgentInputId"));
export type AgentInputId = typeof AgentInputId.Type;
export const AgentEventSequence = count.pipe(Schema.brand("AgentEventSequence"));
export type AgentEventSequence = typeof AgentEventSequence.Type;
export const AgentBinding = Schema.Struct({ host: AgentHost, sessionId: AgentSessionId, incarnation: AgentIncarnationId, generation: AgentGeneration });
export type AgentBinding = typeof AgentBinding.Type;
export const AgentConfig = Schema.Struct({ host: AgentHost, endpointFile: text, tokenFile: text });
export type AgentConfig = typeof AgentConfig.Type;
export const AgentEndpoint = Schema.Struct({
  binding: AgentBinding,
  url: Schema.String.check(Schema.isPattern(/^http:\/\/127\.0\.0\.1:[0-9]+\/mcp$/), Schema.makeFilter((url) => URL.canParse(url) ? undefined : "Valid loopback MCP endpoint required")),
});
const resultIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
export const AgentPrivateResultId = Schema.String.check(Schema.isPattern(resultIdPattern));
export type AgentPrivateResultId = typeof AgentPrivateResultId.Type;
export function agentPrivateResultUri(binding: AgentBinding, resultId: AgentPrivateResultId): string {
  return `agent://${binding.host}/${encodeURIComponent(binding.sessionId)}/${encodeURIComponent(binding.incarnation)}/${binding.generation}/results/${resultId}`;
}
export const AgentPrivateResultReference = Schema.Struct({
  binding: AgentBinding, uri: text, mimeType: Schema.Literal("application/json"),
  byteLength: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: AgentPrivateResultBytes })),
}).check(Schema.makeFilter((reference) => {
  const resultId = reference.uri.slice(reference.uri.lastIndexOf("/") + 1);
  return resultIdPattern.test(resultId) && reference.uri === agentPrivateResultUri(reference.binding, resultId)
    ? undefined : "Private result URI does not match its agent binding";
}));
export type AgentPrivateResultReference = typeof AgentPrivateResultReference.Type;
export const AgentRunOutcomeEnum = Object.freeze({ completed: "completed", aborted: "aborted", error: "error" });
export type AgentRunOutcomeEnum = typeof AgentRunOutcomeEnum[keyof typeof AgentRunOutcomeEnum];
export const AgentRunOutcome = Schema.Enum(AgentRunOutcomeEnum);
export type AgentRunOutcome = typeof AgentRunOutcome.Type;
export const AgentAdmissionDispositionEnum = Object.freeze({ started: "started", queued: "queued", handled: "handled" });
export type AgentAdmissionDispositionEnum = typeof AgentAdmissionDispositionEnum[keyof typeof AgentAdmissionDispositionEnum];
export const AgentAdmissionDisposition = Schema.Enum(AgentAdmissionDispositionEnum);
export type AgentAdmissionDisposition = typeof AgentAdmissionDisposition.Type;
export const AgentInputKindEnum = Object.freeze({ question: "question", approval: "approval" });
export type AgentInputKindEnum = typeof AgentInputKindEnum[keyof typeof AgentInputKindEnum];
export const AgentInputKind = Schema.Enum(AgentInputKindEnum);
export type AgentInputKind = typeof AgentInputKind.Type;
export const AgentRunPhaseEnum = Object.freeze({ retry: "retry", compaction: "compaction", async: "async", continuation: "continuation" });
export type AgentRunPhaseEnum = typeof AgentRunPhaseEnum[keyof typeof AgentRunPhaseEnum];
export const AgentRunPhase = Schema.Enum(AgentRunPhaseEnum);
export type AgentRunPhase = typeof AgentRunPhase.Type;
export const AgentPhaseTransitionEnum = Object.freeze({ started: "started", ended: "ended" });
export type AgentPhaseTransitionEnum = typeof AgentPhaseTransitionEnum[keyof typeof AgentPhaseTransitionEnum];
export const AgentPhaseTransition = Schema.Enum(AgentPhaseTransitionEnum);
export type AgentPhaseTransition = typeof AgentPhaseTransition.Type;
const base = { type: Schema.Literal("agent_lifecycle"), binding: AgentBinding, runId: Schema.NullOr(AgentRunId), at: Schema.Finite };
export const AgentLifecycleEmission = Schema.Union([
  Schema.Struct({ ...base, kind: Schema.Literal("session_started") }),
  Schema.Struct({ ...base, kind: Schema.Literal("session_retired"), reason: Schema.String.check(Schema.isMaxLength(1024)), replacement: Schema.optional(AgentBinding) }),
  Schema.Struct({ ...base, kind: Schema.Literal("submission_admitted"), submissionId: AgentSubmissionId, disposition: AgentAdmissionDisposition }),
  Schema.Struct({ ...base, kind: Schema.Literal("run_started") }),
  Schema.Struct({ ...base, kind: Schema.Literal("run_phase"), phase: AgentRunPhase, transition: AgentPhaseTransition }),
  Schema.Struct({ ...base, kind: Schema.Literal("input_requested"), inputId: AgentInputId, inputKind: AgentInputKind }),
  Schema.Struct({ ...base, kind: Schema.Literal("input_resolved"), inputId: AgentInputId, inputKind: AgentInputKind }),
  Schema.Struct({ ...base, kind: Schema.Literal("run_settled"), outcome: AgentRunOutcome }),
]).pipe(Schema.toTaggedUnion("kind"));
export type AgentLifecycleEmission = typeof AgentLifecycleEmission.Type;
export const AgentLifecycleEvent = AgentLifecycleEmission.mapMembers(Tuple.map(Schema.fieldsAssign({ seq: AgentEventSequence }))).pipe(Schema.toTaggedUnion("kind"));
export type AgentLifecycleEvent = typeof AgentLifecycleEvent.Type;
export const AgentEventPage = Schema.Struct({
  binding: AgentBinding, after: AgentEventSequence, oldestAvailable: AgentEventSequence, highWater: AgentEventSequence,
  entries: Schema.Array(AgentLifecycleEvent).check(Schema.isMaxLength(256)), nextAfter: AgentEventSequence, more: Schema.Boolean,
  gap: Schema.NullOr(Schema.Struct({ from: AgentEventSequence, through: AgentEventSequence })),
}).check(Schema.makeFilter((page) =>
  utf8.encode(encodeJson({ result: { contents: [{
    uri: agentEventsUri(page.binding), mimeType: "application/json",
    text: encodeJson(page),
  }] } })).byteLength <= AgentMcpResultBytes ? undefined : "Agent resource page exceeds encoded byte budget"));
export type AgentEventPage = typeof AgentEventPage.Type;
export function agentEventsUri(binding: AgentBinding): string {
  return `agent://${binding.host}/${encodeURIComponent(binding.sessionId)}/${encodeURIComponent(binding.incarnation)}/${binding.generation}/events`;
}

// Private SDK JSON is validated and bounded; lifecycle never contains this data.
export const boundedJsonSchema = Schema.Json.check(Schema.makeFilter((value) => {
  const stack = [{ value, depth: 0 }];
  let nodes = 0;
  let characters = 0;
  while (stack.length > 0) {
    const item = stack.pop();
    if (!item) break;
    if (++nodes > 65536 || item.depth > 32) return "Agent JSON exceeds structural limits";
    if (typeof item.value === "string") characters += item.value.length;
    else if (typeof item.value === "number" && !Number.isFinite(item.value)) return "Agent JSON requires finite numbers";
    else if (Array.isArray(item.value)) for (const child of item.value) stack.push({ value: child, depth: item.depth + 1 });
    else if (item.value !== null && typeof item.value === "object") for (const [key, child] of Object.entries(item.value)) {
      characters += key.length;
      stack.push({ value: child, depth: item.depth + 1 });
    }
    if (characters > AgentPrivateResultBytes) return "Agent JSON exceeds total character limit";
  }
  return undefined;
}));
const jsonArray = Schema.Array(boundedJsonSchema);
const strings = Schema.Array(text);
const image = Schema.Struct({ type: Schema.Literal("image"), data: Schema.String.check(Schema.isMaxLength(1048576)), mimeType: text });
const images = Schema.Array(image).check(Schema.isMaxLength(16));
const outputStrings = Schema.Array(AgentSdkOutputText);
const outputImages = Schema.Array(image.pipe(Schema.fieldsAssign({ data: AgentSdkOutputText })));
const thinking = Schema.Literals(["inherit", "off", "minimal", "low", "medium", "high", "xhigh", "max", "auto"]);
const queueMode = Schema.Literals(["all", "one-at-a-time"]);
const bound = { binding: AgentBinding };
// Publish safe model metadata only: never headers, endpoint credentials or opaque provider configuration.
export const AgentModel = Schema.Struct({
  id: text, name: text, api: text, provider: text, reasoning: Schema.Boolean,
  input: strings, cost: Schema.Struct({ input: Schema.Finite, output: Schema.Finite, cacheRead: Schema.Finite, cacheWrite: Schema.Finite }),
  contextWindow: Schema.NullOr(count), maxTokens: Schema.NullOr(count),
  premiumMultiplier: Schema.optional(Schema.Finite), preferWebsockets: Schema.optional(Schema.Boolean),
  contextPromotionTarget: Schema.optional(text), priority: Schema.optional(Schema.Int),
});
export type AgentModel = typeof AgentModel.Type;
// Flat agent traversal preserves genuine entry.parentId without recursive tree depth limits.
export const AgentTreeNode = Schema.Struct({ entry: boundedJsonSchema, label: Schema.optional(AgentSdkOutputText), labelTimestamp: Schema.optional(text) });
export type AgentTreeNode = typeof AgentTreeNode.Type;
const todoItem = Schema.Struct({ content: text, status: Schema.Literals(["pending", "in_progress", "completed", "abandoned", "blocked"]), blocker: Schema.optional(text), details: Schema.optional(text), notes: Schema.optional(strings) });
const todoPhase = Schema.Struct({ name: text, tasks: Schema.Array(todoItem) });
const todoPhases = Schema.Array(todoPhase);
const todoOutputPhases = Schema.Array(todoPhase.pipe(Schema.fieldsAssign({
  name: AgentSdkOutputText,
  tasks: Schema.Array(todoItem.pipe(Schema.fieldsAssign({
    content: AgentSdkOutputText, blocker: Schema.optional(AgentSdkOutputText),
    details: Schema.optional(AgentSdkOutputText), notes: Schema.optional(outputStrings),
  }))),
})));
const queuedMessages = Schema.Struct({ steering: outputStrings, followUp: outputStrings });
const contextUsage = Schema.Struct({ tokens: Schema.NullOr(count), contextWindow: count, percent: Schema.NullOr(Schema.Finite) });
const usageLimit = Schema.Union([
  Schema.Struct({ stage: Schema.Literal("low_priority"), resetsAtSec: Schema.Finite, allowanceLeftPercent: Schema.optional(Schema.Int) }),
  Schema.Struct({ stage: Schema.Literal("wrap_up"), resetsAtSec: Schema.optional(Schema.Finite), extraUsage: Schema.Boolean }),
]);
const goal = Schema.Struct({
  id: text, objective: text, status: Schema.Literals(["active", "paused", "budget-limited", "complete", "dropped"]),
  tokenBudget: Schema.optional(count), tokensUsed: count, timeUsedSeconds: Schema.Finite, createdAt: count, updatedAt: count,
});
const goalModeState = Schema.Struct({ enabled: Schema.Boolean, mode: Schema.Literals(["active", "exiting"]), reason: Schema.optional(Schema.Literal("completed")), goal });
export const AgentGetStateInput = Schema.Struct({ binding: Schema.optional(AgentBinding) });
export type AgentGetStateInput = typeof AgentGetStateInput.Type;
export const AgentState = Schema.Struct({
  ...bound, sessionFile: Schema.optional(text), sessionName: Schema.optional(text), cwd: text, model: Schema.NullOr(AgentModel),
  // Public extension contexts do not expose every agent busy/queue field.
  // Null is unknown, never an invented zero used to authorize runner readiness.
  currentRunId: Schema.NullOr(AgentRunId), isStreaming: Schema.Boolean, isCompacting: Schema.NullOr(Schema.Boolean),
  pendingMessageCount: Schema.NullOr(count), pendingAsyncWorkCount: Schema.NullOr(count), messageCount: count, queuedMessages: Schema.optional(queuedMessages),
  thinkingLevel: Schema.optional(thinking), steeringMode: Schema.optional(queueMode), followUpMode: Schema.optional(queueMode),
  interruptMode: Schema.optional(Schema.Literals(["immediate", "wait"])), autoCompactionEnabled: Schema.optional(Schema.Boolean),
  fastModeEnabled: Schema.optional(Schema.Boolean), fastModeActive: Schema.optional(Schema.Boolean), slowModeSupported: Schema.optional(Schema.Boolean),
  slowModeEnabled: Schema.optional(Schema.Boolean), slowModeScope: Schema.optional(Schema.Literals(["session", "global"])),
  usageLimit: Schema.optional(usageLimit), tokensPerSecond: Schema.optional(Schema.NullOr(Schema.Finite)),
  contextUsage: Schema.optional(contextUsage), goal: Schema.optional(Schema.NullOr(goalModeState)),
  systemPrompt: Schema.Union([AgentSdkOutputText, outputStrings]),
  dumpTools: Schema.optional(Schema.Array(Schema.Struct({
    name: AgentSdkOutputText, description: AgentSdkOutputText, parameters: boundedJsonSchema,
    examples: Schema.optional(Schema.Array(Schema.Union([
      Schema.Struct({ caption: Schema.optional(AgentSdkOutputText), call: boundedJsonSchema }),
      Schema.Struct({ caption: Schema.optional(AgentSdkOutputText), bad: boundedJsonSchema, good: boundedJsonSchema }),
      Schema.Struct({ caption: AgentSdkOutputText, note: Schema.optional(AgentSdkOutputText) }),
    ]))),
  }))),
  todoPhases: Schema.optional(todoOutputPhases),
}).check(Schema.makeFilter((state) =>
  state.binding.host === AgentHostEnum.pi
    ? typeof state.systemPrompt === "string" && state.todoPhases === undefined ? undefined : "Pi state requires a string system prompt and has no todo phases"
    : Array.isArray(state.systemPrompt) ? undefined : "OMP state requires actual prompt sections"));
export type AgentState = typeof AgentState.Type;
export const AgentPromptInput = Schema.Struct({ ...bound, submissionId: AgentSubmissionId, message: text, images: Schema.optional(images), streamingBehavior: Schema.optional(Schema.Literals(["steer", "followUp"])) });
export type AgentPromptInput = typeof AgentPromptInput.Type;
export const AgentPromptAdmission = Schema.Struct({ ...bound, submissionId: AgentSubmissionId, disposition: AgentAdmissionDisposition, runId: Schema.NullOr(AgentRunId), eventsUri: text, seq: AgentEventSequence });
export type AgentPromptAdmission = typeof AgentPromptAdmission.Type;
export const AgentPromptResult = Schema.Union([
  Schema.Struct({ admission: AgentPromptAdmission, disposition: Schema.Literal("handled") }),
  Schema.Struct({ admission: AgentPromptAdmission, disposition: Schema.Literal("settled"), final: Schema.Struct({ ...bound, runId: AgentRunId, outcome: AgentRunOutcome }) }),
]);
export type AgentPromptResult = typeof AgentPromptResult.Type;
export const AgentGetMessagesInput = Schema.Struct({ ...bound, cursor: Schema.optional(text), limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 256 }))) });
export type AgentGetMessagesInput = typeof AgentGetMessagesInput.Type;
export const AgentMessagesPage = Schema.Struct({ ...bound, messages: jsonArray.check(Schema.isMaxLength(256)), nextCursor: Schema.NullOr(text), more: Schema.Boolean });
export type AgentMessagesPage = typeof AgentMessagesPage.Type;
export const AgentBashResult = Schema.Struct({ ...bound, exitCode: Schema.NullOr(Schema.Int), output: AgentSdkOutputText, truncated: Schema.Boolean, killed: Schema.Boolean });
export type AgentBashResult = typeof AgentBashResult.Type;
export const AgentAbortResult = Schema.Struct({ ...bound, runId: Schema.NullOr(AgentRunId), disposition: Schema.Literals(["requested", "already_settled"]) });
export type AgentAbortResult = typeof AgentAbortResult.Type;
export class AgentError extends Schema.TaggedError<AgentError>()("AgentError", {
  code: Schema.Literals(["invalid_input", "binding_mismatch", "unsupported_operation", "agent_failure", "resource_gap", "transport_closed", "deadline_exceeded", "sequence_exhausted"]),
  message: Schema.String.check(Schema.isMaxLength(1024)),
}) {}

export type AgentToolResultMetadata = { readonly binding: AgentBinding | undefined; readonly prompt: AgentPromptResult | undefined };

function command<const N extends string, const H extends readonly AgentHost[], I extends Schema.ConstraintCodec<unknown, unknown>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(
  name: N, hosts: H, description: string, input: I, output: O, sensitive = false, approval?: "control",
  inspectOutput: (output: O["Type"]) => AgentToolResultMetadata = () => ({ binding: undefined, prompt: undefined }),
) {
  const outputJson = Schema.fromJsonString(Schema.toCodecJson(output));
  const outputByteLimit = Schema.makeFilter<O["Type"]>((value) =>
    utf8.encode(Schema.encodeSync(outputJson, { onExcessProperty: "error" })(value)).byteLength <= AgentPrivateResultBytes ? undefined : "Agent private result exceeds total byte budget");
  const boundedOutput = output.check(outputByteLimit);
  const structuredOutput = Schema.Union([
    Schema.Struct({ result: boundedOutput }),
    Schema.Struct({ resultResource: AgentPrivateResultReference }),
  ]).check(resultByteLimit);
  const inputDocument = Schema.toJsonSchemaDocument(Schema.toCodecJson(input), { onExcessProperty: "error" });
  const outputDocument = Schema.toJsonSchemaDocument(Schema.toCodecJson(structuredOutput), { onExcessProperty: "error" });
  const inputSchema = Schema.decodeSync(McpSchema.ToolJson)({ ...(Object.keys(inputDocument.definitions).length === 0 ? inputDocument.schema : { ...inputDocument.schema, $defs: inputDocument.definitions }), type: "object" });
  const outputSchema = Schema.decodeUnknownSync(McpSchema.ToolOutputJson)(Object.keys(outputDocument.definitions).length === 0 ? outputDocument.schema : { ...outputDocument.schema, $defs: outputDocument.definitions });
  const decodeOutput = Schema.decodeSync<Schema.fromJsonString<Schema.toCodecJson<O>>>(Schema.fromJsonString(Schema.toCodecJson(output).check(outputByteLimit)), { onExcessProperty: "error" });
  return { name, target: "agent", hosts, description, input, output: boundedOutput, inputSchema, outputSchema, policy: { exposure: "private", sensitive, approval }, structuredOutput, inspectOutput,
    decodeOutput: (text: string) => { const output = decodeOutput(text); return { output, ...inspectOutput(output) }; } };
}
const empty = Schema.Struct(bound);
const done = Schema.Null;
const cancelled = Schema.Struct({ cancelled: Schema.Boolean });
const message = Schema.Struct({ ...bound, message: text, images: Schema.optional(images) });
const enabled = Schema.Struct({ ...bound, enabled: Schema.Boolean });
const enabledResult = Schema.Struct({ enabled: Schema.Boolean });
const instructions = Schema.Struct({ ...bound, customInstructions: Schema.optional(text) });
const branchMessages = Schema.Struct({ messages: Schema.Array(Schema.Struct({ entryId: text, text: AgentSdkOutputText })) });
const cacheMode = Schema.Literals(["off", "streaming", "idle"]);
const subscription = Schema.Literals(["off", "progress", "events"]);
const restored = Schema.Struct({ text: AgentSdkOutputText, images: Schema.optional(outputImages) });
const hostTool = Schema.Struct({ name: text, label: Schema.optional(text), description: text, parameters: boundedJsonSchema, hidden: Schema.optional(Schema.Boolean), loadMode: Schema.optional(Schema.Literals(["essential", "discoverable"])), readsSkillUris: Schema.optional(Schema.Boolean) });
const hostUri = Schema.Struct({ scheme: text, description: Schema.optional(text), writable: Schema.optional(Schema.Boolean), immutable: Schema.optional(Schema.Boolean) });

// Descriptors own wire identity; host-only operations are discovered only on that host.
export const AgentTools = {
  prompt: command("agent.prompt", [AgentHostEnum.pi, AgentHostEnum.omp], "Submit bounded input; returns only after genuine whole-run settlement or local handling.", AgentPromptInput, AgentPromptResult, true, "control", (prompt) => ({ binding: undefined, prompt })),
  getState: command("agent.get_state", [AgentHostEnum.pi, AgentHostEnum.omp], "Read private agent system prompt, tools, todo phases, identity, cwd, safe model metadata and busy counts; optionally validate a known binding, never infer settlement.", AgentGetStateInput, AgentState, true, undefined, (output) => ({ binding: output.binding, prompt: undefined })),
  getMessages: command("agent.get_messages", [AgentHostEnum.pi, AgentHostEnum.omp], "Read a bounded identity-bound page of the private agent transcript.", AgentGetMessagesInput, AgentMessagesPage, true),
  bash: command("agent.bash", [AgentHostEnum.pi, AgentHostEnum.omp], "Run an agent shell command and return bounded actual execution output.", Schema.Struct({ ...bound, command: text, excludeFromContext: Schema.optional(Schema.Boolean) }).check(Schema.makeFilter((input) => input.binding.host === AgentHostEnum.omp && input.excludeFromContext !== undefined ? "excludeFromContext is Pi-only" : undefined, { toJsonSchema: () => [{ if: { properties: { binding: { properties: { host: { const: AgentHostEnum.omp } } } } }, then: { not: { required: ["excludeFromContext"] } } }, true] })), AgentBashResult, true, "control"),
  abort: command("agent.abort", [AgentHostEnum.pi, AgentHostEnum.omp], "Request agent run abort; acknowledgement does not claim terminal settlement.", empty, AgentAbortResult, false, "control"),
  steer: command("agent.steer", [AgentHostEnum.pi, AgentHostEnum.omp], "Queue a steering message into the active owned SDK turn.", message, Schema.Union([Schema.Null, Schema.Struct({ disposition: Schema.Literals(["queued", "handled"]) })]), true),
  followUp: command("agent.follow_up", [AgentHostEnum.pi, AgentHostEnum.omp], "Queue a follow-up message into the active owned SDK turn.", message, Schema.Union([Schema.Null, Schema.Struct({ disposition: Schema.Literals(["queued", "handled"]) })]), true),
  clearQueue: command("agent.clear_queue", [AgentHostEnum.pi], "Clear both agent input queues.", empty, Schema.Struct({ steering: outputStrings, followUp: outputStrings }), true),
  newSession: command("agent.new_session", [AgentHostEnum.pi, AgentHostEnum.omp], "Start a new session.", Schema.Struct({ ...bound, parentSession: Schema.optional(text) }), cancelled),
  setModel: command("agent.set_model", [AgentHostEnum.pi, AgentHostEnum.omp], "Select a model by provider and id.", Schema.Struct({ ...bound, provider: text, modelId: text }), AgentModel),
  cycleModel: command("agent.cycle_model", [AgentHostEnum.pi, AgentHostEnum.omp], "Cycle to the next model.", empty, Schema.NullOr(Schema.Struct({ model: AgentModel, thinkingLevel: Schema.optional(thinking), isScoped: Schema.Boolean }))),
  getAvailableModels: command("agent.get_available_models", [AgentHostEnum.pi, AgentHostEnum.omp], "List available safe model metadata.", empty, Schema.Struct({ models: Schema.Array(AgentModel) })),
  setThinkingLevel: command("agent.set_thinking_level", [AgentHostEnum.pi, AgentHostEnum.omp], "Set the thinking level.", Schema.Struct({ ...bound, level: thinking }).check(Schema.makeFilter((input) => input.binding.host === AgentHostEnum.pi && (input.level === "inherit" || input.level === "auto") ? "Thinking level is not supported by Pi" : undefined, { toJsonSchema: () => [{ if: { properties: { binding: { properties: { host: { const: AgentHostEnum.pi } } } } }, then: { not: { properties: { level: { enum: ["inherit", "auto"] } } } } }, true] })), done),
  cycleThinkingLevel: command("agent.cycle_thinking_level", [AgentHostEnum.pi, AgentHostEnum.omp], "Cycle the thinking level.", empty, Schema.NullOr(Schema.Struct({ level: thinking }))),
  getAvailableThinkingLevels: command("agent.get_available_thinking_levels", [AgentHostEnum.pi, AgentHostEnum.omp], "List selectable thinking levels.", empty, Schema.Struct({ levels: Schema.Array(thinking) })),
  setSteeringMode: command("agent.set_steering_mode", [AgentHostEnum.pi, AgentHostEnum.omp], "Set agent steering queue delivery mode.", Schema.Struct({ ...bound, mode: queueMode }), done),
  setFollowUpMode: command("agent.set_follow_up_mode", [AgentHostEnum.pi, AgentHostEnum.omp], "Set agent follow-up queue delivery mode.", Schema.Struct({ ...bound, mode: queueMode }), done),
  compact: command("agent.compact", [AgentHostEnum.pi, AgentHostEnum.omp], "Compact the agent conversation.", instructions, boundedJsonSchema, true),
  setAutoCompaction: command("agent.set_auto_compaction", [AgentHostEnum.pi, AgentHostEnum.omp], "Enable or disable agent automatic compaction.", enabled, done),
  setAutoRetry: command("agent.set_auto_retry", [AgentHostEnum.pi, AgentHostEnum.omp], "Enable or disable agent automatic retry.", enabled, done),
  abortRetry: command("agent.abort_retry", [AgentHostEnum.pi, AgentHostEnum.omp], "Abort a pending agent retry.", empty, done),
  abortBash: command("agent.abort_bash", [AgentHostEnum.pi, AgentHostEnum.omp], "Abort agent shell execution.", empty, done),
  getSessionStats: command("agent.get_session_stats", [AgentHostEnum.pi, AgentHostEnum.omp], "Read agent session statistics.", empty, boundedJsonSchema),
  exportHtml: command("agent.export_html", [AgentHostEnum.pi, AgentHostEnum.omp], "Export the agent session to HTML.", Schema.Struct({ ...bound, outputPath: Schema.optional(text) }), Schema.Struct({ path: text }), true),
  switchSession: command("agent.switch_session", [AgentHostEnum.pi, AgentHostEnum.omp], "Switch to another session file; optional model override is OMP-only.", Schema.Struct({ ...bound, sessionPath: text, provider: Schema.optional(text), modelId: Schema.optional(text) }).check(Schema.makeFilter((input) => input.binding.host === AgentHostEnum.pi && (input.provider !== undefined || input.modelId !== undefined) ? "Model override is OMP-only" : undefined)), cancelled),
  fork: command("agent.fork", [AgentHostEnum.pi, AgentHostEnum.omp], "Fork at an entry; Pi requires entryId, OMP permits the whole session.", Schema.Struct({ ...bound, entryId: Schema.optional(text) }).check(Schema.makeFilter((input) => input.binding.host === AgentHostEnum.pi && input.entryId === undefined ? "Pi fork requires entryId" : undefined)), Schema.Union([cancelled, Schema.Struct({ text: AgentSdkOutputText, cancelled: Schema.Boolean })]), true),
  clone: command("agent.clone", [AgentHostEnum.pi], "Clone the agent session at its current leaf.", empty, cancelled),
  getForkMessages: command("agent.get_fork_messages", [AgentHostEnum.pi], "List Pi user messages available for forking.", empty, branchMessages, true),
  getEntries: command("agent.get_entries", [AgentHostEnum.pi, AgentHostEnum.omp], "Read append-history strictly after an optional durable entry id.", Schema.Struct({ ...bound, since: Schema.optional(text) }), Schema.Struct({ entries: jsonArray, leafId: Schema.NullOr(text) }), true),
  getTree: command("agent.get_tree", [AgentHostEnum.pi, AgentHostEnum.omp], "Read the agent session tree as flat traversal nodes retaining actual entry.parentId.", empty, Schema.Struct({ tree: Schema.Array(AgentTreeNode), leafId: Schema.NullOr(text) }), true),
  getLastAssistantText: command("agent.get_last_assistant_text", [AgentHostEnum.pi, AgentHostEnum.omp], "Read the last assistant text, if any.", empty, Schema.Struct({ text: Schema.NullOr(AgentSdkOutputText) }), true),
  setSessionName: command("agent.set_session_name", [AgentHostEnum.pi, AgentHostEnum.omp], "Rename the agent session.", Schema.Struct({ ...bound, name: text }), done),
  getCommands: command("agent.get_commands", [AgentHostEnum.pi], "List Pi extension commands, prompt templates and skills.", empty, Schema.Struct({ commands: jsonArray })),
  removeQueuedMessage: command("agent.remove_queued_message", [AgentHostEnum.omp], "Remove one queued message by its queue-chip text.", Schema.Struct({ ...bound, message: text, queue: Schema.Literals(["steering", "followUp"]) }), Schema.Struct({ removed: Schema.Boolean, images: Schema.optional(outputImages), imagesDropped: Schema.optional(Schema.Literal(true)) }), true),
  promoteQueuedMessage: command("agent.promote_queued_message", [AgentHostEnum.omp], "Promote one follow-up message into the steering queue.", Schema.Struct({ ...bound, message: text }), Schema.Struct({ promoted: Schema.Boolean }), true),
  abortAndPrompt: command("agent.abort_and_prompt", [AgentHostEnum.pi, AgentHostEnum.omp], "Abort owned work and admit new input; wait for its genuine settlement.", Schema.Struct({ ...bound, submissionId: AgentSubmissionId, message: text, images: Schema.optional(images) }), AgentPromptResult, true, "control", (prompt) => ({ binding: undefined, prompt })),
  abortAndRestoreQueue: command("agent.abort_and_restore_queue", [AgentHostEnum.omp], "Withdraw queued user input before requesting abort.", empty, Schema.Struct({ steering: Schema.Array(restored), followUp: Schema.Array(restored), imagesDropped: Schema.optional(Schema.Literal(true)), truncated: Schema.optional(Schema.Literal(true)) }), true),
  openSession: command("agent.open_session", [AgentHostEnum.omp], "Continue a session in a directory or start a fresh session.", Schema.Struct({ ...bound, sessionDir: text, provider: Schema.optional(text), modelId: Schema.optional(text) }), Schema.Struct({ cancelled: Schema.Boolean, resumed: Schema.Boolean, sessionId: AgentSessionId, sessionFile: Schema.optional(text) })),
  setFastMode: command("agent.set_fast_mode", [AgentHostEnum.omp], "Set agent fast mode.", enabled, Schema.Struct({ enabled: Schema.Boolean, active: Schema.Boolean })),
  setSlowMode: command("agent.set_slow_mode", [AgentHostEnum.omp], "Set agent slow mode for the active model.", enabled, enabledResult),
  goal: command("agent.goal", [AgentHostEnum.omp], "Read or change goal mode.", Schema.Struct({ ...bound, op: Schema.Literals(["get", "create", "resume", "pause", "drop"]), objective: Schema.optional(text), token_budget: Schema.optional(count) }), Schema.Struct({ goal: Schema.NullOr(boundedJsonSchema), state: Schema.NullOr(boundedJsonSchema) }), true),
  setAskDialog: command("agent.set_ask_dialog", [AgentHostEnum.omp], "Set whether agent ask dialogs are enabled.", enabled, enabledResult),
  getAvailableCommands: command("agent.get_available_commands", [AgentHostEnum.omp], "List the OMP slash-command catalog.", empty, Schema.Struct({ commands: jsonArray })),
  setTodos: command("agent.set_todos", [AgentHostEnum.omp], "Replace agent todo phases.", Schema.Struct({ ...bound, phases: todoPhases }), Schema.Struct({ todoPhases: todoOutputPhases }), true),
  setHostTools: command("agent.set_host_tools", [AgentHostEnum.omp], "Replace host-owned tools reached through the host MCP server.", Schema.Struct({ ...bound, tools: Schema.Array(hostTool) }), Schema.Struct({ toolNames: strings })),
  setHostUriSchemes: command("agent.set_host_uri_schemes", [AgentHostEnum.omp], "Replace host-owned URI schemes reached through the host MCP server.", Schema.Struct({ ...bound, schemes: Schema.Array(hostUri) }), Schema.Struct({ schemes: strings })),
  setSubagentSubscription: command("agent.set_subagent_subscription", [AgentHostEnum.omp], "Select agent subagent observation detail.", Schema.Struct({ ...bound, level: subscription }), Schema.Struct({ level: subscription })),
  getSubagents: command("agent.get_subagents", [AgentHostEnum.omp], "List agent subagent snapshots.", empty, Schema.Struct({ subagents: jsonArray }), true),
  getSubagentMessages: command("agent.get_subagent_messages", [AgentHostEnum.omp], "Read a subagent transcript by id or registered session file.", Schema.Struct({ ...bound, subagentId: Schema.optional(text), sessionFile: Schema.optional(text), fromByte: Schema.optional(count) }), Schema.Struct({ sessionFile: text, fromByte: count, nextByte: count, reset: Schema.Boolean, entries: jsonArray, messages: jsonArray }), true),
  cancelSubagent: command("agent.cancel_subagent", [AgentHostEnum.omp], "Cancel one running agent subagent.", Schema.Struct({ ...bound, subagentId: text }), cancelled),
  steerSubagent: command("agent.steer_subagent", [AgentHostEnum.omp], "Send input to a running agent subagent.", Schema.Struct({ ...bound, subagentId: text, message: text }), done, true),
  liveStart: command("agent.live_start", [AgentHostEnum.omp], "Start agent live voice for this session.", Schema.Struct({ ...bound, voice: Schema.optional(text), instructions: Schema.optional(text) }), Schema.Struct({ voice: text }), true),
  liveStop: command("agent.live_stop", [AgentHostEnum.omp], "Stop agent live voice.", empty, done),
  liveMute: command("agent.live_mute", [AgentHostEnum.omp], "Set microphone mute or toggle it.", Schema.Struct({ ...bound, muted: Schema.optional(Schema.Boolean) }), Schema.Struct({ muted: Schema.Boolean })),
  setInterruptMode: command("agent.set_interrupt_mode", [AgentHostEnum.omp], "Set how steering interrupts agent tools.", Schema.Struct({ ...bound, mode: Schema.Literals(["immediate", "wait"]) }), done),
  setCacheWarming: command("agent.set_cache_warming", [AgentHostEnum.omp], "Set agent session cache-warming mode.", Schema.Struct({ ...bound, mode: cacheMode }), Schema.Struct({ mode: cacheMode })),
  branch: command("agent.branch", [AgentHostEnum.omp], "Branch the OMP agent session at an entry.", Schema.Struct({ ...bound, entryId: text }), Schema.Struct({ text: AgentSdkOutputText, cancelled: Schema.Boolean }), true),
  getBranchMessages: command("agent.get_branch_messages", [AgentHostEnum.omp], "List OMP user messages available for branching.", empty, branchMessages, true),
  handoff: command("agent.handoff", [AgentHostEnum.omp], "Hand the agent conversation to a fresh session.", instructions, Schema.NullOr(Schema.Struct({ savedPath: Schema.optional(text) })), true),
  getMessagesPage: command("agent.get_messages_page", [AgentHostEnum.omp], "Read a bounded identity-bound agent message page.", AgentGetMessagesInput, AgentMessagesPage, true),
  getLoginProviders: command("agent.get_login_providers", [AgentHostEnum.omp], "Privately list OAuth provider authentication status.", empty, Schema.Struct({ providers: Schema.Array(Schema.Struct({ id: text, name: text, available: Schema.Boolean, authenticated: Schema.Boolean })) }), true),
  login: command("agent.login", [AgentHostEnum.omp], "Run private OAuth login using associated standard elicitation.", Schema.Struct({ ...bound, providerId: text }), Schema.Struct({ providerId: text }), true),
  getLogoutAccounts: command("agent.get_logout_accounts", [AgentHostEnum.omp], "Privately list removable credentials for a provider.", Schema.Struct({ ...bound, providerId: text }), Schema.Struct({ accounts: jsonArray }), true),
  logout: command("agent.logout", [AgentHostEnum.omp], "Remove one stored private credential.", Schema.Struct({ ...bound, providerId: text, credentialId: count }), Schema.Struct({ remainingSource: Schema.optional(text) }), true),
  predictWord: command("agent.predict_word", [AgentHostEnum.omp], "Predict a prose suffix at a UTF-16 cursor offset.", Schema.Struct({ ...bound, text, cursor: count }), Schema.Struct({ suffix: Schema.NullOr(AgentSdkOutputText) }), true),
  predictWordFeedback: command("agent.predict_word_feedback", [AgentHostEnum.omp], "Record whether a predicted suggestion was accepted.", Schema.Struct({ ...bound, text, cursor: count, suggestion: text, accepted: Schema.Boolean }), done, true),
  btw: command("agent.btw", [AgentHostEnum.omp], "Ask an agent side question or follow-up in an existing topic.", Schema.Struct({ ...bound, question: text, recordId: Schema.optional(text) }), Schema.Struct({ record: boundedJsonSchema }), true),
  btwCancel: command("agent.btw_cancel", [AgentHostEnum.omp], "Cancel an agent side question.", Schema.Struct({ ...bound, recordId: Schema.optional(text) }), cancelled),
  getBtwHistory: command("agent.get_btw_history", [AgentHostEnum.omp], "Read agent side-question history newest first.", empty, Schema.Struct({ records: jsonArray }), true),
};
export type AgentTool = typeof AgentTools[keyof typeof AgentTools];
export type AgentToolName = AgentTool["name"];
export type AgentToolDescriptor<I extends Schema.ConstraintCodec<{ readonly binding?: AgentBinding | undefined }, Schema.JsonObject>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>> =
  Pick<AgentTool, "name" | "hosts" | "description" | "inputSchema" | "outputSchema"> & {
    readonly input: I;
    readonly output: O;
    readonly structuredOutput: Schema.ConstraintCodec<{ readonly result: O["Type"] } | { readonly resultResource: AgentPrivateResultReference }, unknown>;
    readonly inspectOutput: (output: O["Type"]) => AgentToolResultMetadata;
    readonly decodeOutput: (text: string) => AgentToolResultMetadata & { readonly output: O["Type"] };
  };
export type AgentToolInput<D extends AgentTool> = D["input"]["Type"];
export type AgentToolOutput<D extends AgentTool> = D["output"]["Type"];
export type AgentToolWireResult<D extends AgentTool> = D["structuredOutput"]["Type"];
export type AgentToolHandlers<H extends AgentHost, R = never> = {
  readonly [D in AgentTool as H extends D["hosts"][number] ? D["name"] : never]:
    (input: AgentToolInput<D>, context: McpRequestContext["Service"]) => Effect.Effect<AgentToolOutput<D>, AgentError, R>;
};
export const agentToolDescriptors = Object.values(AgentTools);
export const agentToolDiscovery = new Map<string, AgentTool>(agentToolDescriptors.map((descriptor) => [descriptor.name, descriptor]));
export const AgentToolName = Schema.Literals(agentToolDescriptors.map((descriptor) => descriptor.name));
export const AgentPresentationMetadataKey = "io.coordinator.agent/presentation";
export const AgentHostResultReference = Schema.Struct({
  uri: Schema.String, mimeType: Schema.Literal("application/json"),
  byteLength: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 67108864 })),
});

const presentationSequence = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const presentationKey = Schema.String.check(Schema.isMaxLength(4096));
export const AgentPresentationCursor = Schema.Struct({ binding: AgentBinding, after: presentationSequence });
export type AgentPresentationCursor = typeof AgentPresentationCursor.Type;
export const AgentPresentationPage = Schema.Struct({
  binding: AgentBinding, after: presentationSequence, highWater: presentationSequence,
  nextAfter: presentationSequence, more: Schema.Boolean,
  gap: Schema.NullOr(Schema.Struct({ from: presentationSequence, through: presentationSequence })),
  entries: Schema.Array(Schema.Struct({
    binding: AgentBinding, seq: presentationSequence.check(Schema.isGreaterThan(0)), key: presentationKey,
    snapshot: AgentHostResultReference,
  })).check(Schema.isMaxLength(256)),
});
export type AgentPresentationPage = typeof AgentPresentationPage.Type;
export const AgentPresentationValue = Schema.Struct({
  binding: AgentBinding, seq: presentationSequence.check(Schema.isGreaterThan(0)), key: presentationKey, value: Schema.Json,
});
export type AgentPresentationValue = typeof AgentPresentationValue.Type;
export const agentPresentationUri = (binding: AgentBinding) => agentEventsUri(binding).replace(/\/events$/, "/presentation");

const eventText = Schema.encodeSync(Schema.fromJsonString(AgentLifecycleEvent), { onExcessProperty: "error" });
const escapedText = Schema.encodeSync(Schema.fromJsonString(Schema.String));

export class AgentLifecycleJournal {
  readonly #retention: number;
  readonly #maxPageSize: number;
  readonly #entries: AgentLifecycleEvent[] = [];
  readonly #listeners = new Set<(event: AgentLifecycleEvent) => void>();
  #highWater = AgentEventSequence.make(0);
  #evictedThrough = AgentEventSequence.make(0);
  #fanoutThrough = AgentEventSequence.make(0);
  #fanoutScheduled = false;
  readonly #flushFanout = (): void => {
    const through = this.#highWater;
    // Snapshot retained rows only. Reentrant appends cannot reorder this batch or grow a pending queue.
    const retained = this.#entries.filter((event) => event.seq > this.#fanoutThrough && event.seq <= through);
    const listeners = Array.from(this.#listeners);
    for (const event of retained) {
      for (const listener of listeners) {
        if (!this.#listeners.has(listener)) continue;
        try { listener(event); } catch { /* Subscriber failures cannot roll back committed metadata. */ }
      }
    }
    this.#fanoutThrough = through;
    this.#fanoutScheduled = false;
    if (this.#highWater > through) {
      this.#fanoutScheduled = true;
      queueMicrotask(this.#flushFanout);
    }
  };

  constructor(options: { readonly retention?: number; readonly maxPageSize?: number } = {}) {
    this.#retention = options.retention ?? 1024;
    this.#maxPageSize = options.maxPageSize ?? 256;
    if (!Number.isSafeInteger(this.#retention) || this.#retention < 1 || this.#retention > 65536 ||
        !Number.isSafeInteger(this.#maxPageSize) || this.#maxPageSize < 1 || this.#maxPageSize > 256) {
      throw new AgentError({ code: "invalid_input", message: "Invalid lifecycle journal bounds" });
    }
  }

  get highWater(): AgentEventSequence { return this.#highWater; }

  // SDK adapters derive this typed metadata; unknown SDK/network values are decoded before reaching the journal.
  append(emission: AgentLifecycleEmission): AgentLifecycleEvent {
    if (this.#highWater === Number.MAX_SAFE_INTEGER) {
      throw new AgentError({ code: "sequence_exhausted", message: "Agent lifecycle sequence exhausted" });
    }
    const event: AgentLifecycleEvent = { ...emission, binding: { ...emission.binding },
      ...(emission.kind === "session_retired" && emission.replacement ? { replacement: { ...emission.replacement } } : {}),
      seq: AgentEventSequence.make(this.#highWater + 1) };
    Object.freeze(event.binding);
    if (event.kind === "session_retired" && event.replacement) Object.freeze(event.replacement);
    Object.freeze(event);
    this.#entries.push(event);
    this.#highWater = event.seq;
    if (this.#entries.length > this.#retention) {
      const evicted = this.#entries.shift();
      if (evicted) this.#evictedThrough = evicted.seq;
    }
    // Agent physical fences unwind before any subscriber runs. Fanout is a retained-event hint, not delivery.
    if (!this.#fanoutScheduled) {
      this.#fanoutScheduled = true;
      queueMicrotask(this.#flushFanout);
    }
    return event;
  }

  // Resource handlers decode wire bindings/cursors; the journal enforces retained-history and paging bounds.
  read(binding: AgentBinding, after: AgentEventSequence, limit = this.#maxPageSize): AgentEventPage {
    const identity = { ...binding };
    const cursor = after;
    if (cursor > this.#highWater || !Number.isSafeInteger(limit) || limit < 1 || limit > this.#maxPageSize) {
      throw new AgentError({ code: "invalid_input", message: "Invalid lifecycle resource cursor or page limit" });
    }
    const gap = cursor < this.#evictedThrough ? Object.freeze({ from: AgentEventSequence.make(cursor + 1), through: this.#evictedThrough }) : null;
    const entries: AgentLifecycleEvent[] = [];
    const pageBase = { binding: identity, after: cursor, oldestAvailable: this.#entries[0]?.seq ?? this.#highWater, highWater: this.#highWater, gap };
    const baseBytes = utf8.encode(encodeJson({
      result: { contents: [{ uri: agentEventsUri(identity), mimeType: "application/json",
        text: encodeJson({ ...pageBase, entries: [], nextAfter: this.#highWater, more: false }),
      }] },
    })).byteLength;
    let encodedEntriesBytes = 0;
    let more = false;
    const available = this.#entries.filter((event) => event.seq > cursor && event.binding.host === identity.host &&
      event.binding.sessionId === identity.sessionId && event.binding.incarnation === identity.incarnation && event.binding.generation === identity.generation);
    for (const event of available) {
      if (entries.length === limit) { more = true; break; }
      const entryBytes = utf8.encode(escapedText(eventText(event))).byteLength - 2 + (entries.length === 0 ? 0 : 1);
      const candidateMore = entries.length + 1 < available.length;
      // JSON's true is one byte shorter; the cursor contains only unescaped decimal digits.
      const metadataBytes = candidateMore ? -1 + String(event.seq).length - String(this.#highWater).length : 0;
      if (baseBytes + encodedEntriesBytes + entryBytes + metadataBytes > AgentMcpResultBytes) { more = true; break; }
      encodedEntriesBytes += entryBytes;
      entries.push(event);
    }
    const last = entries.at(-1);
    const nextAfter = more && last ? last.seq : this.#highWater;
    return Object.freeze({ ...pageBase, binding: Object.freeze(identity),
      entries: Object.freeze(entries), nextAfter, more });
  }

  subscribe(listener: (event: AgentLifecycleEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }
}
