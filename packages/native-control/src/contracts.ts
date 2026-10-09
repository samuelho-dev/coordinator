import { Effect, Schema, Tuple } from "effect";
import type { McpRequestContext } from "effect/ai/McpSchema";

export const NativeMcpProtocolVersion = "2025-11-25";
export const NativePrivateChunkBytes = 32 * 1024;
export const NativeMcpWireBytes = 64 * 1024;
export const NativeMcpResultBytes = NativeMcpWireBytes - 1024;
export const NativePrivateResultBytes = 64 * 1024 * 1024;
const utf8 = new TextEncoder();
export const resultByteLimit = Schema.makeFilter<unknown>((value) =>
  utf8.encode(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(value)).byteLength <= NativeMcpResultBytes ? undefined : "Native result exceeds encoded byte budget",
  // The wire union has only object branches; preserve its root type for standard MCP discovery.
  { toJsonSchema: () => [{ type: "object" }, true] });

const identifier = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9_.:-]{1,160}$/));
const count = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const text = Schema.String.check(Schema.isMaxLength(16000));
export const NativeSdkOutputText = Schema.String.check(Schema.isMaxLength(NativePrivateResultBytes));
export const NativeHost = Schema.Literals(["pi", "omp"]);
export type NativeHost = typeof NativeHost.Type;
export const NativeSessionId = identifier.pipe(Schema.brand("NativeSessionId"));
export type NativeSessionId = typeof NativeSessionId.Type;
export const NativeIncarnationId = identifier.pipe(Schema.brand("NativeIncarnationId"));
export type NativeIncarnationId = typeof NativeIncarnationId.Type;
export const NativeGeneration = count.pipe(Schema.brand("NativeGeneration"));
export type NativeGeneration = typeof NativeGeneration.Type;
export const NativeRunId = identifier.pipe(Schema.brand("NativeRunId"));
export type NativeRunId = typeof NativeRunId.Type;
export const NativeSubmissionId = identifier.pipe(Schema.brand("NativeSubmissionId"));
export type NativeSubmissionId = typeof NativeSubmissionId.Type;
export const NativeInputId = identifier.pipe(Schema.brand("NativeInputId"));
export type NativeInputId = typeof NativeInputId.Type;
export const NativeEventSequence = count.pipe(Schema.brand("NativeEventSequence"));
export type NativeEventSequence = typeof NativeEventSequence.Type;
export const NativeBinding = Schema.Struct({ host: NativeHost, sessionId: NativeSessionId, incarnation: NativeIncarnationId, generation: NativeGeneration });
export type NativeBinding = typeof NativeBinding.Type;
export const NativePluginConfig = Schema.Struct({ host: NativeHost, endpointFile: text, tokenFile: text });
export type NativePluginConfig = typeof NativePluginConfig.Type;
export const NativePluginEndpoint = Schema.Struct({
  binding: NativeBinding,
  url: Schema.String.check(Schema.isPattern(/^http:\/\/127\.0\.0\.1:[0-9]+\/mcp$/), Schema.makeFilter((url) => URL.canParse(url) ? undefined : "Valid loopback MCP endpoint required")),
});
const resultIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
export const NativePrivateResultId = Schema.String.check(Schema.isPattern(resultIdPattern));
export type NativePrivateResultId = typeof NativePrivateResultId.Type;
export function nativePrivateResultUri(binding: NativeBinding, resultId: NativePrivateResultId): string {
  return `native://${binding.host}/${encodeURIComponent(binding.sessionId)}/${encodeURIComponent(binding.incarnation)}/${binding.generation}/results/${resultId}`;
}
export const NativePrivateResultReference = Schema.Struct({
  binding: NativeBinding, uri: text, mimeType: Schema.Literal("application/json"),
  byteLength: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: NativePrivateResultBytes })),
}).check(Schema.makeFilter((reference) => {
  const resultId = reference.uri.slice(reference.uri.lastIndexOf("/") + 1);
  return resultIdPattern.test(resultId) && reference.uri === nativePrivateResultUri(reference.binding, resultId)
    ? undefined : "Private result URI does not match its native binding";
}));
export type NativePrivateResultReference = typeof NativePrivateResultReference.Type;
const outcome = Schema.Literals(["completed", "aborted", "error"]);
const disposition = Schema.Literals(["started", "queued", "handled"]);
const inputKind = Schema.Literals(["question", "approval"]);
const base = { type: Schema.Literal("native_lifecycle"), binding: NativeBinding, runId: Schema.NullOr(NativeRunId), at: Schema.Finite };
export const NativeLifecycleEmission = Schema.Union([
  Schema.Struct({ ...base, kind: Schema.Literal("session_started") }),
  Schema.Struct({ ...base, kind: Schema.Literal("session_retired"), reason: Schema.String.check(Schema.isMaxLength(1024)), replacement: Schema.optional(NativeBinding) }),
  Schema.Struct({ ...base, kind: Schema.Literal("submission_admitted"), submissionId: NativeSubmissionId, disposition }),
  Schema.Struct({ ...base, kind: Schema.Literal("run_started") }),
  Schema.Struct({ ...base, kind: Schema.Literal("run_phase"), phase: Schema.Literals(["retry", "compaction", "async", "continuation"]), transition: Schema.Literals(["started", "ended"]) }),
  Schema.Struct({ ...base, kind: Schema.Literal("input_requested"), inputId: NativeInputId, inputKind }),
  Schema.Struct({ ...base, kind: Schema.Literal("input_resolved"), inputId: NativeInputId, inputKind }),
  Schema.Struct({ ...base, kind: Schema.Literal("run_settled"), outcome }),
]);
export type NativeLifecycleEmission = typeof NativeLifecycleEmission.Type;
export const NativeLifecycleEvent = NativeLifecycleEmission.mapMembers(Tuple.map(Schema.fieldsAssign({ seq: NativeEventSequence })));
export type NativeLifecycleEvent = typeof NativeLifecycleEvent.Type;
export const NativeEventPage = Schema.Struct({
  binding: NativeBinding, after: NativeEventSequence, oldestAvailable: NativeEventSequence, highWater: NativeEventSequence,
  entries: Schema.Array(NativeLifecycleEvent).check(Schema.isMaxLength(256)), nextAfter: NativeEventSequence, more: Schema.Boolean,
  gap: Schema.NullOr(Schema.Struct({ from: NativeEventSequence, through: NativeEventSequence })),
}).check(Schema.makeFilter((page) =>
  utf8.encode(Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))({ result: { contents: [{
    uri: nativeEventsUri(page.binding), mimeType: "application/json",
    text: Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(page),
  }] } })).byteLength <= NativeMcpResultBytes ? undefined : "Native resource page exceeds encoded byte budget"));
export type NativeEventPage = typeof NativeEventPage.Type;
export function nativeEventsUri(binding: NativeBinding): string {
  return `native://${binding.host}/${encodeURIComponent(binding.sessionId)}/${encodeURIComponent(binding.incarnation)}/${binding.generation}/events`;
}

// Private SDK JSON is validated and bounded; lifecycle never contains this data.
export const boundedJsonSchema = Schema.Json.check(Schema.makeFilter((value) => {
  const stack = [{ value, depth: 0 }];
  let nodes = 0;
  let characters = 0;
  while (stack.length > 0) {
    const item = stack.pop();
    if (!item) break;
    if (++nodes > 65536 || item.depth > 32) return "Native JSON exceeds structural limits";
    if (typeof item.value === "string") characters += item.value.length;
    else if (typeof item.value === "number" && !Number.isFinite(item.value)) return "Native JSON requires finite numbers";
    else if (Array.isArray(item.value)) for (const child of item.value) stack.push({ value: child, depth: item.depth + 1 });
    else if (item.value !== null && typeof item.value === "object") for (const [key, child] of Object.entries(item.value)) {
      characters += key.length;
      stack.push({ value: child, depth: item.depth + 1 });
    }
    if (characters > NativePrivateResultBytes) return "Native JSON exceeds total character limit";
  }
  return undefined;
}));
const jsonArray = Schema.Array(boundedJsonSchema);
const strings = Schema.Array(text);
const image = Schema.Struct({ type: Schema.Literal("image"), data: Schema.String.check(Schema.isMaxLength(1048576)), mimeType: text });
const images = Schema.Array(image).check(Schema.isMaxLength(16));
const outputStrings = Schema.Array(NativeSdkOutputText);
const outputImages = Schema.Array(image.pipe(Schema.fieldsAssign({ data: NativeSdkOutputText })));
const thinking = Schema.Literals(["inherit", "off", "minimal", "low", "medium", "high", "xhigh", "max", "auto"]);
const queueMode = Schema.Literals(["all", "one-at-a-time"]);
const bound = { binding: NativeBinding };
// Publish safe model metadata only: never headers, endpoint credentials or opaque provider configuration.
export const NativeModel = Schema.Struct({
  id: text, name: text, api: text, provider: text, reasoning: Schema.Boolean,
  input: strings, cost: Schema.Struct({ input: Schema.Finite, output: Schema.Finite, cacheRead: Schema.Finite, cacheWrite: Schema.Finite }),
  contextWindow: Schema.NullOr(count), maxTokens: Schema.NullOr(count),
  premiumMultiplier: Schema.optional(Schema.Finite), preferWebsockets: Schema.optional(Schema.Boolean),
  contextPromotionTarget: Schema.optional(text), priority: Schema.optional(Schema.Int),
});
export type NativeModel = typeof NativeModel.Type;
// Flat native traversal preserves genuine entry.parentId without recursive tree depth limits.
export const NativeTreeNode = Schema.Struct({ entry: boundedJsonSchema, label: Schema.optional(NativeSdkOutputText), labelTimestamp: Schema.optional(text) });
export type NativeTreeNode = typeof NativeTreeNode.Type;
const todoItem = Schema.Struct({ content: text, status: Schema.Literals(["pending", "in_progress", "completed", "abandoned", "blocked"]), blocker: Schema.optional(text), details: Schema.optional(text), notes: Schema.optional(strings) });
const todoPhase = Schema.Struct({ name: text, tasks: Schema.Array(todoItem) });
const todoPhases = Schema.Array(todoPhase);
const todoOutputPhases = Schema.Array(todoPhase.pipe(Schema.fieldsAssign({
  name: NativeSdkOutputText,
  tasks: Schema.Array(todoItem.pipe(Schema.fieldsAssign({
    content: NativeSdkOutputText, blocker: Schema.optional(NativeSdkOutputText),
    details: Schema.optional(NativeSdkOutputText), notes: Schema.optional(outputStrings),
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
export const NativeGetStateInput = Schema.Struct({ binding: Schema.optional(NativeBinding) });
export type NativeGetStateInput = typeof NativeGetStateInput.Type;
export const NativeState = Schema.Struct({
  ...bound, sessionFile: Schema.optional(text), sessionName: Schema.optional(text), cwd: text, model: Schema.NullOr(NativeModel),
  // Public extension contexts do not expose every native busy/queue field.
  // Null is unknown, never an invented zero used to authorize runner readiness.
  currentRunId: Schema.NullOr(NativeRunId), isStreaming: Schema.Boolean, isCompacting: Schema.NullOr(Schema.Boolean),
  pendingMessageCount: Schema.NullOr(count), pendingAsyncWorkCount: Schema.NullOr(count), messageCount: count, queuedMessages: Schema.optional(queuedMessages),
  thinkingLevel: Schema.optional(thinking), steeringMode: Schema.optional(queueMode), followUpMode: Schema.optional(queueMode),
  interruptMode: Schema.optional(Schema.Literals(["immediate", "wait"])), autoCompactionEnabled: Schema.optional(Schema.Boolean),
  fastModeEnabled: Schema.optional(Schema.Boolean), fastModeActive: Schema.optional(Schema.Boolean), slowModeSupported: Schema.optional(Schema.Boolean),
  slowModeEnabled: Schema.optional(Schema.Boolean), slowModeScope: Schema.optional(Schema.Literals(["session", "global"])),
  usageLimit: Schema.optional(usageLimit), tokensPerSecond: Schema.optional(Schema.NullOr(Schema.Finite)),
  contextUsage: Schema.optional(contextUsage), goal: Schema.optional(Schema.NullOr(goalModeState)),
  systemPrompt: Schema.Union([NativeSdkOutputText, outputStrings]),
  dumpTools: Schema.optional(Schema.Array(Schema.Struct({
    name: NativeSdkOutputText, description: NativeSdkOutputText, parameters: boundedJsonSchema,
    examples: Schema.optional(Schema.Array(Schema.Union([
      Schema.Struct({ caption: Schema.optional(NativeSdkOutputText), call: boundedJsonSchema }),
      Schema.Struct({ caption: Schema.optional(NativeSdkOutputText), bad: boundedJsonSchema, good: boundedJsonSchema }),
      Schema.Struct({ caption: NativeSdkOutputText, note: Schema.optional(NativeSdkOutputText) }),
    ]))),
  }))),
  todoPhases: Schema.optional(todoOutputPhases),
}).check(Schema.makeFilter((state) =>
  state.binding.host === "pi"
    ? typeof state.systemPrompt === "string" && state.todoPhases === undefined ? undefined : "Pi state requires a string system prompt and has no todo phases"
    : Array.isArray(state.systemPrompt) ? undefined : "OMP state requires actual prompt sections"));
export type NativeState = typeof NativeState.Type;
export const NativePromptInput = Schema.Struct({ ...bound, submissionId: NativeSubmissionId, message: text, images: Schema.optional(images), streamingBehavior: Schema.optional(Schema.Literals(["steer", "followUp"])) });
export type NativePromptInput = typeof NativePromptInput.Type;
export const NativePromptAdmission = Schema.Struct({ ...bound, submissionId: NativeSubmissionId, disposition, runId: Schema.NullOr(NativeRunId), eventsUri: text, seq: NativeEventSequence });
export type NativePromptAdmission = typeof NativePromptAdmission.Type;
export const NativePromptResult = Schema.Union([
  Schema.Struct({ admission: NativePromptAdmission, disposition: Schema.Literal("handled") }),
  Schema.Struct({ admission: NativePromptAdmission, disposition: Schema.Literal("settled"), final: Schema.Struct({ ...bound, runId: NativeRunId, outcome }) }),
]);
export type NativePromptResult = typeof NativePromptResult.Type;
export const NativeGetMessagesInput = Schema.Struct({ ...bound, cursor: Schema.optional(text), limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 256 }))) });
export type NativeGetMessagesInput = typeof NativeGetMessagesInput.Type;
export const NativeMessagesPage = Schema.Struct({ ...bound, messages: jsonArray.check(Schema.isMaxLength(256)), nextCursor: Schema.NullOr(text), more: Schema.Boolean });
export type NativeMessagesPage = typeof NativeMessagesPage.Type;
export const NativeBashResult = Schema.Struct({ ...bound, exitCode: Schema.NullOr(Schema.Int), output: NativeSdkOutputText, truncated: Schema.Boolean, killed: Schema.Boolean });
export type NativeBashResult = typeof NativeBashResult.Type;
export const NativeAbortResult = Schema.Struct({ ...bound, runId: Schema.NullOr(NativeRunId), disposition: Schema.Literals(["requested", "already_settled"]) });
export type NativeAbortResult = typeof NativeAbortResult.Type;
export class NativeControlError extends Schema.TaggedError<NativeControlError>()("NativeControlError", {
  code: Schema.Literals(["invalid_input", "binding_mismatch", "unsupported_operation", "native_failure", "resource_gap", "transport_closed", "deadline_exceeded", "sequence_exhausted"]),
  message: Schema.String.check(Schema.isMaxLength(1024)),
}) {}

function command<const H extends readonly NativeHost[], I extends Schema.ConstraintCodec<unknown, unknown>, O extends Schema.Top & Schema.ConstraintCodec<unknown, unknown>>(
  hosts: H, description: string, input: I, output: O, sensitive = false, approval?: "control",
) {
  const outputJson = Schema.fromJsonString(Schema.toCodecJson(output));
  const boundedOutput = output.check(Schema.makeFilter<unknown>((value) =>
    utf8.encode(Schema.encodeUnknownSync(outputJson, { onExcessProperty: "error" })(value)).byteLength <= NativePrivateResultBytes ? undefined : "Native private result exceeds total byte budget"));
  const structuredOutput = Schema.Union([
    Schema.Struct({ result: boundedOutput }),
    Schema.Struct({ resultResource: NativePrivateResultReference }),
  ]).check(resultByteLimit);
  return { target: "native", hosts, description, input, output: boundedOutput, policy: { exposure: "private", sensitive, approval }, structuredOutput };
}
const empty = Schema.Struct(bound);
const done = Schema.Null;
const cancelled = Schema.Struct({ cancelled: Schema.Boolean });
const message = Schema.Struct({ ...bound, message: text, images: Schema.optional(images) });
const enabled = Schema.Struct({ ...bound, enabled: Schema.Boolean });
const enabledResult = Schema.Struct({ enabled: Schema.Boolean });
const instructions = Schema.Struct({ ...bound, customInstructions: Schema.optional(text) });
const branchMessages = Schema.Struct({ messages: Schema.Array(Schema.Struct({ entryId: text, text: NativeSdkOutputText })) });
const cacheMode = Schema.Literals(["off", "streaming", "idle"]);
const subscription = Schema.Literals(["off", "progress", "events"]);
const restored = Schema.Struct({ text: NativeSdkOutputText, images: Schema.optional(outputImages) });
const hostTool = Schema.Struct({ name: text, label: Schema.optional(text), description: text, parameters: boundedJsonSchema, hidden: Schema.optional(Schema.Boolean), loadMode: Schema.optional(Schema.Literals(["essential", "discoverable"])), readsSkillUris: Schema.optional(Schema.Boolean) });
const hostUri = Schema.Struct({ scheme: text, description: Schema.optional(text), writable: Schema.optional(Schema.Boolean), immutable: Schema.optional(Schema.Boolean) });

// Keys alone define MCP tool identity. Host-only operations are discovered only on that host.
export const NativeTools = {
  "native.prompt": command(["pi", "omp"], "Submit bounded input; returns only after genuine whole-run settlement or local handling.", NativePromptInput, NativePromptResult, true, "control"),
  "native.get_state": command(["pi", "omp"], "Read private native system prompt, tools, todo phases, identity, cwd, safe model metadata and busy counts; optionally validate a known binding, never infer settlement.", NativeGetStateInput, NativeState, true),
  "native.get_messages": command(["pi", "omp"], "Read a bounded identity-bound page of the private native transcript.", NativeGetMessagesInput, NativeMessagesPage, true),
  "native.bash": command(["pi", "omp"], "Run a native shell command and return bounded actual execution output.", Schema.Struct({ ...bound, command: text, excludeFromContext: Schema.optional(Schema.Boolean) }).check(Schema.makeFilter((input) => input.binding.host === "omp" && input.excludeFromContext !== undefined ? "excludeFromContext is Pi-only" : undefined, { toJsonSchema: () => [{ if: { properties: { binding: { properties: { host: { const: "omp" } } } } }, then: { not: { required: ["excludeFromContext"] } } }, true] })), NativeBashResult, true, "control"),
  "native.abort": command(["pi", "omp"], "Request native run abort; acknowledgement does not claim terminal settlement.", empty, NativeAbortResult, false, "control"),
  "native.steer": command(["pi", "omp"], "Queue a steering message.", message, Schema.Union([Schema.Null, Schema.Struct({ disposition: Schema.Literals(["queued", "handled"]) })]), true),
  "native.follow_up": command(["pi", "omp"], "Queue a follow-up message.", message, Schema.Union([Schema.Null, Schema.Struct({ disposition: Schema.Literals(["queued", "handled"]) })]), true),
  "native.clear_queue": command(["pi"], "Clear both native input queues.", empty, Schema.Struct({ steering: outputStrings, followUp: outputStrings }), true),
  "native.new_session": command(["pi", "omp"], "Start a new session.", Schema.Struct({ ...bound, parentSession: Schema.optional(text) }), cancelled),
  "native.set_model": command(["pi", "omp"], "Select a model by provider and id.", Schema.Struct({ ...bound, provider: text, modelId: text }), NativeModel),
  "native.cycle_model": command(["pi", "omp"], "Cycle to the next model.", empty, Schema.NullOr(Schema.Struct({ model: NativeModel, thinkingLevel: Schema.optional(thinking), isScoped: Schema.Boolean }))),
  "native.get_available_models": command(["pi", "omp"], "List available safe model metadata.", empty, Schema.Struct({ models: Schema.Array(NativeModel) })),
  "native.set_thinking_level": command(["pi", "omp"], "Set the thinking level.", Schema.Struct({ ...bound, level: thinking }).check(Schema.makeFilter((input) => input.binding.host === "pi" && (input.level === "inherit" || input.level === "auto") ? "Thinking level is not supported by Pi" : undefined, { toJsonSchema: () => [{ if: { properties: { binding: { properties: { host: { const: "pi" } } } } }, then: { not: { properties: { level: { enum: ["inherit", "auto"] } } } } }, true] })), done),
  "native.cycle_thinking_level": command(["pi", "omp"], "Cycle the thinking level.", empty, Schema.NullOr(Schema.Struct({ level: thinking }))),
  "native.get_available_thinking_levels": command(["pi", "omp"], "List selectable thinking levels.", empty, Schema.Struct({ levels: Schema.Array(thinking) })),
  "native.set_steering_mode": command(["pi", "omp"], "Set native steering queue delivery mode.", Schema.Struct({ ...bound, mode: queueMode }), done),
  "native.set_follow_up_mode": command(["pi", "omp"], "Set native follow-up queue delivery mode.", Schema.Struct({ ...bound, mode: queueMode }), done),
  "native.compact": command(["pi", "omp"], "Compact the native conversation.", instructions, boundedJsonSchema, true),
  "native.set_auto_compaction": command(["pi", "omp"], "Enable or disable native automatic compaction.", enabled, done),
  "native.set_auto_retry": command(["pi", "omp"], "Enable or disable native automatic retry.", enabled, done),
  "native.abort_retry": command(["pi", "omp"], "Abort a pending native retry.", empty, done),
  "native.abort_bash": command(["pi", "omp"], "Abort native shell execution.", empty, done),
  "native.get_session_stats": command(["pi", "omp"], "Read native session statistics.", empty, boundedJsonSchema),
  "native.export_html": command(["pi", "omp"], "Export the native session to HTML.", Schema.Struct({ ...bound, outputPath: Schema.optional(text) }), Schema.Struct({ path: text }), true),
  "native.switch_session": command(["pi", "omp"], "Switch to another session file; optional model override is OMP-only.", Schema.Struct({ ...bound, sessionPath: text, provider: Schema.optional(text), modelId: Schema.optional(text) }).check(Schema.makeFilter((input) => input.binding.host === "pi" && (input.provider !== undefined || input.modelId !== undefined) ? "Model override is OMP-only" : undefined)), cancelled),
  "native.fork": command(["pi", "omp"], "Fork at an entry; Pi requires entryId, OMP permits the whole session.", Schema.Struct({ ...bound, entryId: Schema.optional(text) }).check(Schema.makeFilter((input) => input.binding.host === "pi" && input.entryId === undefined ? "Pi fork requires entryId" : undefined)), Schema.Union([cancelled, Schema.Struct({ text: NativeSdkOutputText, cancelled: Schema.Boolean })]), true),
  "native.clone": command(["pi"], "Clone the native session at its current leaf.", empty, cancelled),
  "native.get_fork_messages": command(["pi"], "List Pi user messages available for forking.", empty, branchMessages, true),
  "native.get_entries": command(["pi", "omp"], "Read append-history strictly after an optional durable entry id.", Schema.Struct({ ...bound, since: Schema.optional(text) }), Schema.Struct({ entries: jsonArray, leafId: Schema.NullOr(text) }), true),
  "native.get_tree": command(["pi", "omp"], "Read the native session tree as flat traversal nodes retaining actual entry.parentId.", empty, Schema.Struct({ tree: Schema.Array(NativeTreeNode), leafId: Schema.NullOr(text) }), true),
  "native.get_last_assistant_text": command(["pi", "omp"], "Read the last assistant text, if any.", empty, Schema.Struct({ text: Schema.NullOr(NativeSdkOutputText) }), true),
  "native.set_session_name": command(["pi", "omp"], "Rename the native session.", Schema.Struct({ ...bound, name: text }), done),
  "native.get_commands": command(["pi"], "List Pi extension commands, prompt templates and skills.", empty, Schema.Struct({ commands: jsonArray })),
  "native.remove_queued_message": command(["omp"], "Remove one queued message by its queue-chip text.", Schema.Struct({ ...bound, message: text, queue: Schema.Literals(["steering", "followUp"]) }), Schema.Struct({ removed: Schema.Boolean, images: Schema.optional(outputImages), imagesDropped: Schema.optional(Schema.Literal(true)) }), true),
  "native.promote_queued_message": command(["omp"], "Promote one follow-up message into the steering queue.", Schema.Struct({ ...bound, message: text }), Schema.Struct({ promoted: Schema.Boolean }), true),
  "native.abort_and_prompt": command(["omp"], "Abort current work and admit new input; wait for its genuine settlement.", Schema.Struct({ ...bound, submissionId: NativeSubmissionId, message: text, images: Schema.optional(images) }), NativePromptResult, true, "control"),
  "native.abort_and_restore_queue": command(["omp"], "Withdraw queued user input before requesting abort.", empty, Schema.Struct({ steering: Schema.Array(restored), followUp: Schema.Array(restored), imagesDropped: Schema.optional(Schema.Literal(true)), truncated: Schema.optional(Schema.Literal(true)) }), true),
  "native.open_session": command(["omp"], "Continue a session in a directory or start a fresh session.", Schema.Struct({ ...bound, sessionDir: text, provider: Schema.optional(text), modelId: Schema.optional(text) }), Schema.Struct({ cancelled: Schema.Boolean, resumed: Schema.Boolean, sessionId: NativeSessionId, sessionFile: Schema.optional(text) })),
  "native.set_fast_mode": command(["omp"], "Set native fast mode.", enabled, Schema.Struct({ enabled: Schema.Boolean, active: Schema.Boolean })),
  "native.set_slow_mode": command(["omp"], "Set native slow mode for the active model.", enabled, enabledResult),
  "native.goal": command(["omp"], "Read or change goal mode.", Schema.Struct({ ...bound, op: Schema.Literals(["get", "create", "resume", "pause", "drop"]), objective: Schema.optional(text), token_budget: Schema.optional(count) }), Schema.Struct({ goal: Schema.NullOr(boundedJsonSchema), state: Schema.NullOr(boundedJsonSchema) }), true),
  "native.set_ask_dialog": command(["omp"], "Set whether native ask dialogs are enabled.", enabled, enabledResult),
  "native.get_available_commands": command(["omp"], "List the OMP slash-command catalog.", empty, Schema.Struct({ commands: jsonArray })),
  "native.set_todos": command(["omp"], "Replace native todo phases.", Schema.Struct({ ...bound, phases: todoPhases }), Schema.Struct({ todoPhases: todoOutputPhases }), true),
  "native.set_host_tools": command(["omp"], "Replace host-owned tools reached through the host MCP server.", Schema.Struct({ ...bound, tools: Schema.Array(hostTool) }), Schema.Struct({ toolNames: strings })),
  "native.set_host_uri_schemes": command(["omp"], "Replace host-owned URI schemes reached through the host MCP server.", Schema.Struct({ ...bound, schemes: Schema.Array(hostUri) }), Schema.Struct({ schemes: strings })),
  "native.set_subagent_subscription": command(["omp"], "Select native subagent observation detail.", Schema.Struct({ ...bound, level: subscription }), Schema.Struct({ level: subscription })),
  "native.get_subagents": command(["omp"], "List native subagent snapshots.", empty, Schema.Struct({ subagents: jsonArray }), true),
  "native.get_subagent_messages": command(["omp"], "Read a subagent transcript by id or registered session file.", Schema.Struct({ ...bound, subagentId: Schema.optional(text), sessionFile: Schema.optional(text), fromByte: Schema.optional(count) }), Schema.Struct({ sessionFile: text, fromByte: count, nextByte: count, reset: Schema.Boolean, entries: jsonArray, messages: jsonArray }), true),
  "native.cancel_subagent": command(["omp"], "Cancel one running native subagent.", Schema.Struct({ ...bound, subagentId: text }), cancelled),
  "native.steer_subagent": command(["omp"], "Send input to a running native subagent.", Schema.Struct({ ...bound, subagentId: text, message: text }), done, true),
  "native.live_start": command(["omp"], "Start native live voice for this session.", Schema.Struct({ ...bound, voice: Schema.optional(text), instructions: Schema.optional(text) }), Schema.Struct({ voice: text }), true),
  "native.live_stop": command(["omp"], "Stop native live voice.", empty, done),
  "native.live_mute": command(["omp"], "Set microphone mute or toggle it.", Schema.Struct({ ...bound, muted: Schema.optional(Schema.Boolean) }), Schema.Struct({ muted: Schema.Boolean })),
  "native.set_interrupt_mode": command(["omp"], "Set how steering interrupts native tools.", Schema.Struct({ ...bound, mode: Schema.Literals(["immediate", "wait"]) }), done),
  "native.set_cache_warming": command(["omp"], "Set native session cache-warming mode.", Schema.Struct({ ...bound, mode: cacheMode }), Schema.Struct({ mode: cacheMode })),
  "native.branch": command(["omp"], "Branch the OMP native session at an entry.", Schema.Struct({ ...bound, entryId: text }), Schema.Struct({ text: NativeSdkOutputText, cancelled: Schema.Boolean }), true),
  "native.get_branch_messages": command(["omp"], "List OMP user messages available for branching.", empty, branchMessages, true),
  "native.handoff": command(["omp"], "Hand the native conversation to a fresh session.", instructions, Schema.NullOr(Schema.Struct({ savedPath: Schema.optional(text) })), true),
  "native.get_messages_page": command(["omp"], "Read a bounded identity-bound native message page.", NativeGetMessagesInput, NativeMessagesPage, true),
  "native.get_login_providers": command(["omp"], "Privately list OAuth provider authentication status.", empty, Schema.Struct({ providers: Schema.Array(Schema.Struct({ id: text, name: text, available: Schema.Boolean, authenticated: Schema.Boolean })) }), true),
  "native.login": command(["omp"], "Run private OAuth login using associated standard elicitation.", Schema.Struct({ ...bound, providerId: text }), Schema.Struct({ providerId: text }), true),
  "native.get_logout_accounts": command(["omp"], "Privately list removable credentials for a provider.", Schema.Struct({ ...bound, providerId: text }), Schema.Struct({ accounts: jsonArray }), true),
  "native.logout": command(["omp"], "Remove one stored private credential.", Schema.Struct({ ...bound, providerId: text, credentialId: count }), Schema.Struct({ remainingSource: Schema.optional(text) }), true),
  "native.predict_word": command(["omp"], "Predict a prose suffix at a UTF-16 cursor offset.", Schema.Struct({ ...bound, text, cursor: count }), Schema.Struct({ suffix: Schema.NullOr(NativeSdkOutputText) }), true),
  "native.predict_word_feedback": command(["omp"], "Record whether a predicted suggestion was accepted.", Schema.Struct({ ...bound, text, cursor: count, suggestion: text, accepted: Schema.Boolean }), done, true),
  "native.btw": command(["omp"], "Ask a native side question or follow-up in an existing topic.", Schema.Struct({ ...bound, question: text, recordId: Schema.optional(text) }), Schema.Struct({ record: boundedJsonSchema }), true),
  "native.btw_cancel": command(["omp"], "Cancel a native side question.", Schema.Struct({ ...bound, recordId: Schema.optional(text) }), cancelled),
  "native.get_btw_history": command(["omp"], "Read native side-question history newest first.", empty, Schema.Struct({ records: jsonArray }), true),
};
export type NativeToolName = keyof typeof NativeTools;
export type NativeToolInput<N extends NativeToolName> = typeof NativeTools[N]["input"]["Type"];
export type NativeToolOutput<N extends NativeToolName> = typeof NativeTools[N]["output"]["Type"];
export type NativeToolWireResult<N extends NativeToolName> = typeof NativeTools[N]["structuredOutput"]["Type"];
export type NativeToolHandlers<H extends NativeHost, R = never> = {
  readonly [N in NativeToolName as H extends typeof NativeTools[N]["hosts"][number] ? N : never]:
    (input: NativeToolInput<N>, context: McpRequestContext["Service"]) => Effect.Effect<NativeToolOutput<N>, NativeControlError, R>;
};
export function nativeToolInputSchema<N extends NativeToolName>(name: N): typeof NativeTools[N]["input"] { return NativeTools[name].input; }
export function nativeToolOutputSchema<N extends NativeToolName>(name: N): typeof NativeTools[N]["output"] { return NativeTools[name].output; }
export function nativeToolWireSchema<N extends NativeToolName>(name: N): typeof NativeTools[N]["structuredOutput"] { return NativeTools[name].structuredOutput; }
