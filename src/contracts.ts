import { Effect, Schema, Struct } from "effect";
import { Tool } from "effect/ai";
import { AgentBinding, AgentRunId, AgentSubmissionId, AgentEventSequence, AgentState, AgentGetMessagesInput, AgentMessagesPage, AgentAbortResult, AgentHost, AgentRunOutcome, AgentLifecycleEmission } from "./agent/protocol.ts";
import { TaskId, PeerId, SessionId, RunnerTask, Archive, Evidence, Peer } from "./generated/database/types.ts";
import { TaskState, TaskStateEnum, RunnerState, RunnerStateEnum } from "./generated/database/enums.ts";

export const MachineId = Schema.String.pipe(Schema.brand("MachineId"));
export type MachineId = typeof MachineId.Type;
export const WorkerId = Schema.String.pipe(Schema.brand("WorkerId"));
export type WorkerId = typeof WorkerId.Type;
export const RunnerId = Schema.String.pipe(Schema.brand("RunnerId"));
export type RunnerId = typeof RunnerId.Type;
export const GitHubRepositoryId = Schema.Int.pipe(Schema.brand("GitHubRepositoryId"));
export type GitHubRepositoryId = typeof GitHubRepositoryId.Type;
export const GitHubRunId = Schema.Int.pipe(Schema.brand("GitHubRunId"));
export type GitHubRunId = typeof GitHubRunId.Type;
export const GitHubWorkflowId = Schema.Int.pipe(Schema.brand("GitHubWorkflowId"));
export type GitHubWorkflowId = typeof GitHubWorkflowId.Type;
export const GitHubJobId = Schema.Int.pipe(Schema.brand("GitHubJobId"));
export type GitHubJobId = typeof GitHubJobId.Type;
export const GitHubPullRequestNumber = Schema.Int.pipe(Schema.brand("GitHubPullRequestNumber"));
export type GitHubPullRequestNumber = typeof GitHubPullRequestNumber.Type;
export const UserId = Schema.Int.pipe(Schema.brand("UserId"));
export type UserId = typeof UserId.Type;
export const Text = Schema.String.check(Schema.isMaxLength(16000));
export const Repo = Schema.String.check(Schema.isPattern(/^[a-z0-9.-]+\/[a-z0-9_.-]+\/[a-z0-9_.-]+$/)).pipe(Schema.brand("@samuelho/coordinator/Repo"));
export const JsonValue = Schema.MutableJson;
export type JsonValue = typeof JsonValue.Type;
export const Json = Schema.Record(Schema.String, Schema.mutableKey(JsonValue));
export type Json = typeof Json.Type;
const strings = Schema.mutable(Schema.Array(Text));
const boundedStrings = strings.check(Schema.isMaxLength(32));
export const ArchiveParticipants = Schema.mutable(Schema.Array(Peer.fields.sender));
const port = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 65535 }));
const hash = RunnerTask.fields.fingerprint.check(Schema.isPattern(/^[a-f0-9]{64}$/));

export const Address = Schema.Struct({
  messageId: PeerId, revision: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000000 })),
  workerId: WorkerId, sessionId: SessionId, incarnation: RunnerTask.fields.incarnation, repo: Repo, ref: Text, sha: Evidence.fields.sha,
  scope: Text, restrictions: Text, expectedEvidence: boundedStrings, deadline: Schema.Finite,
});
export type Address = typeof Address.Type;
export const Work = Address.mapFields(Struct.omit(["messageId", "revision", "sessionId", "incarnation"])).pipe(Schema.fieldsAssign({
  taskId: TaskId, launcher: AgentHost, prompt: Text,
  githubRepoId: GitHubRepositoryId, requiredLanes: boundedStrings,
  bypassPeer: Schema.Boolean, approved: Schema.Literal(true),
}));
export type Work = typeof Work.Type;
export const ReportState = Schema.Literals([TaskStateEnum.RUNNING, TaskStateEnum.SUCCEEDED, TaskStateEnum.FAILED, TaskStateEnum.HELD]);
export enum DispositionEnum {
  ready = "ready",
  unable = "unable",
  clarify = "clarify",
  hold = "hold",
}
export const Disposition = Schema.Enum(DispositionEnum);
export const RunnerReportStates: Partial<Record<RunnerStateEnum, ReportState>> = {
  [RunnerStateEnum.RUNNING]: TaskStateEnum.RUNNING,
  [RunnerStateEnum.SUCCEEDED]: TaskStateEnum.SUCCEEDED,
  [RunnerStateEnum.FAILED]: TaskStateEnum.FAILED,
  [RunnerStateEnum.HELD]: TaskStateEnum.HELD,
};
export const EvidenceTarget = Schema.Struct({ sha: Evidence.fields.sha, ref: Text, repositoryId: GitHubRepositoryId, requiredLanes: strings });
export type EvidenceTarget = typeof EvidenceTarget.Type;
export const Instruction = Schema.Struct({ prompt: Text, address: Schema.mutableKey(Address) });
export type Instruction = typeof Instruction.Type;
export enum RunnerRejectionStateEnum {
  full = "full",
  notAccepted = "not-accepted",
}
export const RunnerRejectionState = Schema.Enum(RunnerRejectionStateEnum);
export type RunnerRejectionState = typeof RunnerRejectionState.Type;
const RunnerAcceptanceState = Schema.Union([RunnerState, RunnerRejectionState]);
export const ReconciledRunner = Schema.Struct({ runnerId: RunnerId, state: RunnerAcceptanceState, observedAt: Schema.Finite });
export const EvidenceObservation = Evidence.mapFields(Struct.pick(["kind", "sha", "principal"])).pipe(Schema.fieldsAssign({ kind: Text, data: JsonValue, observedAt: Schema.Finite, stale: Schema.Boolean }));
export type EvidenceObservation = typeof EvidenceObservation.Type;
export enum OfferDecisionEnum {
  pending = "pending",
  accepted = "accepted",
  declined = "declined",
  expired = "expired",
  bypass = "bypass",
}
export const OfferDecision = Schema.Enum(OfferDecisionEnum);
export type OfferDecision = typeof OfferDecision.Type;
export const Task = Schema.Struct({
  taskId: TaskId, owner: Peer.fields.owner, fingerprint: hash, request: Work, state: Schema.mutableKey(TaskState),
  runnerId: Schema.mutableKey(Schema.optional(RunnerId)), peerSession: Schema.mutableKey(Schema.optional(SessionId)), peerIncarnation: Schema.mutableKey(Schema.optional(RunnerTask.fields.incarnation)),
  offerExpires: Schema.mutableKey(Schema.optional(Schema.Finite)),
  offerDecision: Schema.mutableKey(Schema.optional(OfferDecision)),
  address: Schema.mutableKey(Schema.optional(Address)), readback: Schema.mutableKey(Schema.optional(Address)), confirmedRevision: Schema.mutableKey(Schema.optional(Schema.Int)),
  disposition: Schema.mutableKey(Schema.optional(Disposition)), reason: Schema.mutableKey(Schema.optional(Text)), nextCheck: Schema.mutableKey(Schema.optional(Schema.Finite)),
  result: Schema.mutableKey(Schema.optional(JsonValue)), createdAt: Schema.Finite, updatedAt: Schema.mutableKey(Schema.Finite),
  evidence: Schema.mutableKey(Schema.optional(Schema.mutable(Schema.Array(EvidenceObservation)))),
  evidenceTarget: Schema.mutableKey(Schema.optional(EvidenceTarget)), instruction: Schema.mutableKey(Schema.optional(Instruction)), reconciledRunner: Schema.mutableKey(Schema.optional(ReconciledRunner)),
});
export type Task = typeof Task.Type;
const acceptanceFields = { taskId: TaskId, worktree: Schema.optional(Text), address: Schema.optional(Address), reason: Schema.optional(Text) };
export const Acceptance = Schema.Union([
  Schema.Struct({ ...acceptanceFields, accepted: Schema.Literal(true), state: RunnerState }),
  Schema.Struct({ ...acceptanceFields, accepted: Schema.Literal(false), state: RunnerRejectionState }),
]);
export type Acceptance = typeof Acceptance.Type;
export enum RoleEnum {
  coordinator = "coordinator",
  session = "session",
  runner = "runner",
  archiveWriter = "archive-writer",
  reviewer = "reviewer",
  admin = "admin",
}
export const Role = Schema.Enum(RoleEnum);
export const Principal = Schema.Struct({ id: Peer.fields.sender, owner: Peer.fields.owner, tokenHash: hash, roles: Schema.Array(Role), repos: Schema.Array(Repo), runnerUrl: Schema.optional(Text), controlTokenHash: Schema.optional(hash) });
export type Principal = typeof Principal.Type;
export const ObjectBackupConfig = Schema.Struct({ endpoint: Text, region: Schema.String, bucket: Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/)), accessKeyFile: Text, secretKeyFile: Text });
export type ObjectBackupConfig = typeof ObjectBackupConfig.Type;
const HttpsObjectBackupConfig = Schema.Struct({
  ...ObjectBackupConfig.fields,
  endpoint: Text.check(Schema.isPattern(/^https:\/\/[^/?#@\\\s]+\/?$/i), Schema.makeFilter((value) => URL.canParse(value) ? undefined : "Valid HTTPS object backup authority required")),
});
export const SnapshotKey = Schema.String.check(Schema.isPattern(/^sanitized\/[0-9]{13}\.pgdump$/)).pipe(Schema.brand("@samuelho/coordinator/SnapshotKey"));
export type SnapshotKey = typeof SnapshotKey.Type;
export const BrokerConfig = Schema.Struct({ host: Text, port, principals: Schema.mutableKey(Schema.Array(Principal)), backupDirectory: Text, archiveRetentionMs: Schema.Finite, backupRetentionMs: Schema.Finite, objectBackup: Schema.optional(HttpsObjectBackupConfig), githubTokenEnv: Schema.optional(Text), repositoryIds: Schema.Record(Repo, GitHubRepositoryId.check(Schema.isGreaterThan(0))) });
export type BrokerConfig = typeof BrokerConfig.Type;
export const RunnerConfig = Schema.Struct({
  id: RunnerId, brokerUrl: Schema.String, tokenFile: Schema.String, host: Schema.String, port, controlTokenHash: hash,
  stateDirectory: Schema.String, slots: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })),
  repositories: Schema.Record(Schema.String.pipe(Schema.brand("@samuelho/coordinator/Repo")), Schema.String), launchers: Schema.Record(Schema.String, Schema.String),
  modelDirectory: Schema.String, childDirectory: Schema.String,
  childUidBase: UserId.check(Schema.isBetween({ minimum: 1000000, maximum: 0xffffffff - 1000000 })),
});
export type RunnerConfig = typeof RunnerConfig.Type;
export const ConnectHosts = Schema.Record(Schema.String, Schema.String);
export const PluginConfig = Schema.Struct({ brokerUrl: Schema.String, tokenFile: Schema.String, machineId: MachineId, stateDirectory: Schema.String, selectedRemote: Schema.optional(Schema.String), host: AgentHost, connectHosts: Schema.optional(ConnectHosts) });
export type PluginConfig = typeof PluginConfig.Type;

export const Session = Schema.Struct({ sessionId: SessionId, incarnation: RunnerTask.fields.incarnation, machineId: MachineId, host: AgentHost, repo: Repo, workerId: Schema.optional(WorkerId) });
export type Session = typeof Session.Type;
export const SessionKey = Session.mapFields(Struct.pick(["sessionId", "incarnation"]));
export type SessionKey = typeof SessionKey.Type;
export const TaskKey = Work.mapFields(Struct.pick(["taskId"]));
export type TaskKey = typeof TaskKey.Type;
export const Delivery = Schema.Struct({ messageId: PeerId, incarnation: RunnerTask.fields.incarnation, payload: JsonValue, expiresAt: Schema.Finite });
export type Delivery = typeof Delivery.Type;
export const Poll = Schema.Struct({ messages: Schema.mutable(Schema.Array(Delivery)), offers: Schema.mutable(Schema.Array(Task)), releases: Schema.mutable(Schema.Array(Task)), archiveEnabled: Schema.optional(Schema.Boolean) });
export type Poll = typeof Poll.Type;

// Only privacy-projected metadata is archive content; peer/agent control payloads are separate JSON.
export const ArchivePayload = Schema.Union([
  Schema.Struct({ id: Archive.fields.entry, parentId: Schema.NullOr(Archive.fields.entry), type: Schema.String.check(Schema.isPattern(/^[a-z_]{1,40}$/)), role: Schema.optional(Schema.Literals(["user", "assistant", "toolResult"])), contentOmitted: Schema.Literal(true) }),
  Schema.Struct({ coverageOnly: Schema.Literal(true), gaps: boundedStrings }),
]);
export type ArchivePayload = typeof ArchivePayload.Type;
export const ArchiveEntry = Archive.mapFields(Struct.pick(["entry", "digest"])).mapFields(Struct.renameKeys({ entry: "entryId" })).pipe(Schema.fieldsAssign({ digest: hash, payload: ArchivePayload, gaps: strings }));
export type ArchiveEntry = typeof ArchiveEntry.Type;
export const ArchiveRow = ArchiveEntry.pipe(Schema.fieldsAssign({ sequence: Schema.Int, createdAt: Schema.Finite }));
export type ArchiveRow = typeof ArchiveRow.Type;
export const InboxEntry = Schema.Struct({ payload: JsonValue, digest: hash, receivedAt: Schema.Finite });
export type InboxEntry = typeof InboxEntry.Type;
export const Inbox = Schema.Record(PeerId, Schema.mutableKey(InboxEntry));

export const Job = Schema.Struct({ id: GitHubJobId, name: Text, status: Text, conclusion: Schema.NullOr(Text), started_at: Schema.NullOr(Text), completed_at: Schema.NullOr(Text) });
export const CiLane = Schema.Struct({ lane: Text, runId: Schema.optional(GitHubRunId), attempt: Schema.optional(Schema.Int), status: Schema.optional(Text), conclusion: Schema.optional(Schema.NullOr(Text)), jobs: Schema.optional(Schema.mutable(Schema.Array(Job))) });
export enum CiVerdictEnum {
  green = "green",
  pending = "pending",
  failed = "failed",
  unknown = "unknown",
}
export const CiVerdict = Schema.Enum(CiVerdictEnum);
export type CiVerdict = typeof CiVerdict.Type;
export const CiEvidence = Schema.Struct({ sha: Evidence.fields.sha, repositoryId: GitHubRepositoryId, ref: Text, observedAt: Text, provenance: Text, verdict: Schema.mutableKey(CiVerdict), reason: Schema.mutableKey(Schema.optional(Text)), lanes: Schema.mutable(Schema.Array(CiLane)) });
export type CiEvidence = typeof CiEvidence.Type;
export const LocalEvidence = Schema.Struct({ taskId: TaskId, sha: Evidence.fields.sha, commands: strings, passed: Schema.Boolean });
export type LocalEvidence = typeof LocalEvidence.Type;
export const DeploymentEvidence = Schema.Struct({ taskId: TaskId, sha: Evidence.fields.sha, environment: Text, url: Text, observedState: Text, provenance: Text });
export type DeploymentEvidence = typeof DeploymentEvidence.Type;
export enum ReviewDecisionEnum {
  approved = "approved",
  changesRequested = "changes-requested",
}
export const ReviewDecision = Schema.Enum(ReviewDecisionEnum);
export type ReviewDecision = typeof ReviewDecision.Type;
export const ReviewEvidence = Schema.Struct({ taskId: TaskId, sha: Evidence.fields.sha, decision: ReviewDecision, note: Text });
export type ReviewEvidence = typeof ReviewEvidence.Type;

export const AgentIdentity = Session.mapFields(Struct.omit(["host", "workerId"])).pipe(Schema.fieldsAssign({ ref: Work.fields.ref, sha: Work.fields.sha, workerId: Work.fields.workerId, workerIdentitySource: Schema.Literal("agent-session") }));
export type AgentIdentity = typeof AgentIdentity.Type;
export const RunnerInspect = Schema.Struct({ ...TaskKey.fields, cursor: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))) });
export type RunnerInspect = typeof RunnerInspect.Type;
export enum RunnerControlEnum {
  getState = "get_state",
  getMessages = "get_messages",
  abort = "abort",
}
export const RunnerControl = Schema.Enum(RunnerControlEnum);
export type RunnerControl = typeof RunnerControl.Type;
export const RunnerCommand = Schema.Union([
  Schema.Struct({ ...TaskKey.fields, type: Schema.Literal(RunnerControlEnum.getState) }),
  Schema.Struct({ ...TaskKey.fields, type: Schema.Literal(RunnerControlEnum.getMessages), cursor: AgentGetMessagesInput.fields.cursor, limit: AgentGetMessagesInput.fields.limit }),
  Schema.Struct({ ...TaskKey.fields, type: Schema.Literal(RunnerControlEnum.abort) }),
]).pipe(Schema.toTaggedUnion("type"));
export type RunnerCommand = typeof RunnerCommand.Type;
export const RunnerPrepare = Schema.Struct({ ...TaskKey.fields, address: Address });
export type RunnerPrepare = typeof RunnerPrepare.Type;
export const RunnerRelease = Schema.Struct({ ...RunnerPrepare.fields, prompt: Work.fields.prompt });
export type RunnerRelease = typeof RunnerRelease.Type;
export const RunnerReleased = Schema.Struct({ released: Schema.Literal(true) });
export type RunnerReleased = typeof RunnerReleased.Type;
export const RunnerEvent = Schema.Struct({ type: Schema.String.check(Schema.isPattern(/^[a-z_]{1,40}$/)), contentOmitted: Schema.optional(Schema.Literal(true)), address: Schema.optional(Address), state: Schema.optional(RunnerState), status: Schema.optional(AgentRunOutcome), fingerprint: Schema.optional(hash), digest: Schema.optional(hash), id: Schema.optional(AgentSubmissionId), reason: Schema.optional(Text), before: Schema.optional(Schema.Int), seq: Schema.optional(Schema.Int), binding: Schema.optional(AgentBinding), runId: Schema.optional(Schema.NullOr(AgentRunId)), agentCursor: Schema.optional(AgentEventSequence), kind: Schema.optional(Schema.Literals(AgentLifecycleEmission.discriminants)) });
export type RunnerEvent = typeof RunnerEvent.Type;
export const RunnerResult = Schema.NullOr(Schema.Struct({ launched: Schema.optional(Schema.Boolean), hostSettled: Schema.optional(Schema.Boolean), evidenceVerified: Schema.optional(Schema.Boolean), exitCode: Schema.optional(Schema.Int), signal: Schema.optional(Schema.String.check(Schema.isPattern(/^SIG[A-Z0-9]{1,16}$/))) }));
export type RunnerResult = typeof RunnerResult.Type;
const inspectionFields = { events: Schema.mutable(Schema.Array(RunnerEvent)), nextCursor: Schema.Int };
export const RunnerInspection = Schema.Union([
  Acceptance.members[0].pipe(Schema.fieldsAssign(inspectionFields)),
  Acceptance.members[1].pipe(Schema.fieldsAssign(inspectionFields)),
]);
export type RunnerInspection = typeof RunnerInspection.Type;
export const RunnerAgentState = AgentState.mapFields(Struct.pick(["sessionFile", "cwd", "currentRunId", "isStreaming", "isCompacting", "pendingMessageCount", "pendingAsyncWorkCount", "messageCount"])).pipe(Schema.fieldsAssign({ binding: AgentBinding }));
export const AgentResponse = Schema.Union([RunnerAgentState, AgentMessagesPage, AgentAbortResult, Schema.Struct({ type: Schema.Literal("gap"), reason: Text })]);

export const ModelProfile = Schema.Struct({ modelRoles: Schema.Struct({ default: Schema.String }) });
export type ModelProfile = typeof ModelProfile.Type;

const ok = Schema.Struct({ ok: Schema.Literal(true) });
const recorded = Schema.Struct({ recorded: Schema.Literal(true) });
export const Health = Schema.Struct({ status: Schema.Literal("ok") });
export type Health = typeof Health.Type;
export type CommandTarget = "broker" | "runner" | "shared" | "plugin" | "program";
export type CommandExposure = "tool" | "internal" | "local" | "private";
type CommandPolicy = {
  readonly exposure: CommandExposure;
  readonly transaction?: "transaction" | "remote";
  readonly offer?: true;
  readonly approval?: "exact-work" | "exact-instruction" | "release" | "handoff" | "participant-consent" | "archive-consent" | "control";
};
// Capabilities capture the authenticated principal or active host context at composition time.
// Executors never import a service, cast a callback, or discard its error/environment channels.
export function command<const N extends string, const T extends CommandTarget, const K extends string, I extends Schema.ConstraintCodec<unknown, unknown>, O extends Schema.ConstraintCodec<unknown, unknown>, const P extends CommandPolicy>(
  name: N, target: T, capability: K, description: string, input: I, output: O, policy: P,
) {
  return {
    name, target, capability, description, input, output,
    request: Schema.Struct({ method: Schema.Literal(name), params: input }),
    transaction: policy.transaction, offer: policy.offer ?? false, approval: policy.approval,
    ...policy,
    structuredOutput: Schema.Struct({ result: output }),
    executeDecoded<E, R>(capabilities: Record<K, (params: I["Type"]) => Effect.Effect<O["Type"], E, R>>, params: I["Type"]) {
      return capabilities[capability](params);
    },
    execute<E, R>(
      capabilities: Record<K, (params: I["Type"]) => Effect.Effect<O["Type"], E, R>>,
      params: unknown,
    ) {
      return Schema.decodeUnknownEffect(input, { onExcessProperty: "error" })(params).pipe(
        Effect.mapError(() => new HttpError({ status: 400, message: "Invalid command input or output" })),
        Effect.flatMap((decoded) => capabilities[capability](decoded)),
      );
    },
  };
}
const empty = Tool.EmptyParams;
export enum SessionLivenessEnum {
  live = "live",
  stale = "stale",
}
export const SessionLiveness = Schema.Enum(SessionLivenessEnum);
export type SessionLiveness = typeof SessionLiveness.Type;
const sessionList = Schema.mutable(Schema.Array(Schema.Struct({ ...Session.fields, status: SessionLiveness, liveUntil: Schema.Finite })));
const sessionPage = Schema.Struct({ sessions: sessionList, nextCursor: Schema.optional(SessionId) });
const pluginReadback = Schema.Struct({ ...RunnerPrepare.fields, reason: Schema.optional(Text), nextCheck: Schema.optional(Schema.Finite) });
export const Commands = {
  health: command("health", "shared", "health", "Read authenticated service health.", empty, Health, { exposure: "internal" }),
  listSessions: command("session.list", "broker", "listSessions", "List authenticated same-owner sessions across repositories with current liveness in bounded pages.", Schema.Struct({ cursor: Schema.optional(SessionId) }), sessionPage, { exposure: "tool", transaction: "transaction" }),
  heartbeatSession: command("session.heartbeat", "broker", "heartbeatSession", "Renew the verified agent session lease without replacing a live incarnation.", Session, Schema.Struct({ liveUntil: Schema.Finite }), { exposure: "internal", transaction: "transaction" }),
  pollSession: command("session.poll", "broker", "pollSession", "Fetch owner- and incarnation-scoped peer messages, offers and releases.", SessionKey, Poll, { exposure: "internal", transaction: "transaction" }),
  stopSession: command("session.stop", "broker", "stopSession", "Revoke the bound session and its peer deliveries.", SessionKey, Schema.Struct({ stopped: Schema.Literal(true) }), { exposure: "internal", transaction: "transaction" }),
  sendPeer: command("peer.send", "broker", "sendPeer", "Commit an expiring addressed message to a verified recipient; commitment is not delivery.", Schema.Struct({ messageId: PeerId, ...SessionKey.fields, payload: JsonValue, ttlMs: Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 60000 })) }), Schema.Struct({ delivered: Schema.Literal(false), committed: Schema.Literal(true), messageId: PeerId }), { exposure: "tool", transaction: "transaction" }),
  acknowledgePeer: command("peer.ack", "broker", "acknowledgePeer", "Acknowledge a message for the exact live session incarnation.", Schema.Struct({ ...SessionKey.fields, messageId: PeerId }), Schema.Struct({ handled: Schema.Literal(true) }), { exposure: "internal", transaction: "transaction" }),
  replyPeer: command("peer.reply", "broker", "replyPeer", "Accept or decline an unexpired peer offer for the bound agent session.", Schema.Struct({ ...SessionKey.fields, ...TaskKey.fields, decision: Schema.Literals([OfferDecisionEnum.accepted, OfferDecisionEnum.declined]) }), Task, { exposure: "internal", transaction: "transaction", offer: true }),
  submitTask: command("task.submit", "broker", "submitTask", "Record exact human-approved bounded work, preserving idempotent task identity.", Work, Task, { exposure: "tool", transaction: "transaction", offer: true, approval: "exact-work" }),
  getTask: command("task.get", "broker", "getTask", "Read an authorized task and current evidence observations, including staleness.", TaskKey, Task, { exposure: "tool", transaction: "transaction" }),
  attachTask: command("task.attach", "broker", "attachTask", "Read projected runner journal events as the task coordinator.", RunnerInspect, RunnerInspection, { exposure: "tool", transaction: "remote" }),
  controlTask: command("task.control", "broker", "controlTask", "Control the assigned child: get_messages is sensitive and abort requires approval.", RunnerCommand, AgentResponse, { exposure: "tool", transaction: "remote", approval: "control" }),
  instructTask: command("task.instruction", "broker", "instructTask", "Set an exact human-approved later instruction for an accepted agent peer.", Schema.Struct({ ...RunnerPrepare.fields, prompt: Text, approved: Schema.Literal(true) }), Task, { exposure: "tool", transaction: "transaction", approval: "exact-instruction" }),
  addressTask: command("task.address", "broker", "addressTask", "Bind an increased address revision while preserving approved work and reconciling runner readback.", RunnerPrepare, Task, { exposure: "tool", transaction: "remote" }),
  readbackTask: command("task.readback", "broker", "readbackTask", "Record the bound agent peer's actual readiness or reasoned disposition.", Schema.Struct({ ...RunnerPrepare.fields, disposition: Disposition, reason: Schema.optional(Text), nextCheck: Schema.optional(Schema.Finite) }), Task, { exposure: "internal", transaction: "transaction" }),
  confirmTask: command("task.confirm", "broker", "confirmTask", "Explicitly confirm matching ready readback and release the approved instruction before its deadline.", Schema.Struct({ ...TaskKey.fields, revision: Schema.Int }), Task, { exposure: "tool", transaction: "remote", approval: "release" }),
  handoffTask: command("task.handoff", "broker", "handoffTask", "Rebind to a verified agent recipient only after old ownership has safely reconciled.", Schema.Struct({ ...TaskKey.fields, oldSessionId: SessionId, oldIncarnation: RunnerTask.fields.incarnation, newSessionId: SessionId, newIncarnation: RunnerTask.fields.incarnation, revision: Schema.Int }), Task, { exposure: "tool", transaction: "remote", approval: "handoff" }),
  reportTask: command("task.report", "broker", "reportTask", "Record a confirmed addressed agent peer's state and result without terminal regression.", Schema.Struct({ ...SessionKey.fields, ...TaskKey.fields, state: ReportState, result: JsonValue }), Task, { exposure: "internal", transaction: "transaction" }),
  heartbeatRunner: command("runner.heartbeat", "broker", "heartbeatRunner", "Renew the enrolled runner capacity lease; durable outcomes are reported separately.", Schema.Struct({ runnerId: RunnerId, slots: RunnerConfig.fields.slots, load: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100000 })) }), ok, { exposure: "internal", transaction: "transaction" }),
  readbackRunner: command("runner.readback", "broker", "readbackRunner", "Persist assigned runner readback bound to the actual child session.", Schema.Struct({ runnerId: RunnerId, ...RunnerPrepare.fields }), ok, { exposure: "internal", transaction: "transaction" }),
  reportRunner: command("runner.report", "broker", "reportRunner", "Persist assigned runner state and result without terminal regression.", Schema.Struct({ runnerId: RunnerId, ...TaskKey.fields, state: ReportState, result: JsonValue }), ok, { exposure: "internal", transaction: "transaction" }),
  targetEvidence: command("evidence.target", "broker", "targetEvidence", "Set the SHA/ref/CI observation target; this does not authorize execution.", Schema.Struct({ ...TaskKey.fields, sha: Evidence.fields.sha, ref: Text, requiredLanes: strings.check(Schema.isBetweenLength(1, 32)) }), Schema.Struct({ target: EvidenceTarget, executionAuthorized: Schema.Literal(false) }), { exposure: "tool", transaction: "transaction" }),
  collectCiEvidence: command("evidence.ci", "broker", "collectCiEvidence", "Collect CI observations for the task's evidence target with enrolled provenance.", TaskKey, CiEvidence, { exposure: "tool", transaction: "remote" }),
  recordLocalEvidence: command("evidence.local", "broker", "recordLocalEvidence", "Record caller-asserted commands and results; this operation does not run checks.", LocalEvidence, recorded, { exposure: "tool", transaction: "transaction" }),
  recordDeploymentEvidence: command("evidence.deployment", "broker", "recordDeploymentEvidence", "Record a caller-supplied deployment observation; this operation does not deploy.", DeploymentEvidence, recorded, { exposure: "tool", transaction: "transaction" }),
  recordReviewEvidence: command("evidence.review", "broker", "recordReviewEvidence", "Record an enrolled reviewer's decision with trusted reviewer identity.", ReviewEvidence, recorded, { exposure: "tool", transaction: "transaction" }),
  consentParticipant: command("archive.participantConsent", "broker", "consentParticipant", "Enroll or revoke the authenticated participant's consent after explicit human consent.", Schema.Struct({ owner: Peer.fields.owner, sessionId: SessionId, enabled: Schema.Boolean }), Schema.Struct({ participant: Peer.fields.sender, enabled: Schema.Boolean }), { exposure: "internal", transaction: "transaction", approval: "participant-consent" }),
  consentArchive: command("archive.consent", "broker", "consentArchive", "Set bound session archive enrollment with separately consenting participants.", Schema.Struct({ sessionId: SessionId, participants: ArchiveParticipants.check(Schema.isMaxLength(32)), enabled: Schema.Boolean }), Schema.Struct({ enabled: Schema.Boolean }), { exposure: "internal", transaction: "transaction", approval: "archive-consent" }),
  appendArchive: command("archive.append", "broker", "appendArchive", "Append only privacy-projected journal metadata under active consent.", Schema.Struct({ sessionId: SessionId, ...ArchiveEntry.fields, gaps: boundedStrings }), Schema.Struct({ appended: Schema.Literal(true), digest: hash }), { exposure: "internal", transaction: "transaction" }),
  listArchive: command("archive.list", "broker", "listArchive", "Read authorized sanitized archive metadata in bounded pages.", Schema.Struct({ sessionId: SessionId, cursor: Schema.optional(Schema.Int) }), Schema.mutable(Schema.Array(ArchiveRow)), { exposure: "tool", transaction: "transaction" }),
  deleteArchive: command("archive.delete", "broker", "deleteArchive", "Revoke archive enrollment and delete server rows; local optout additionally erases the journal.", Schema.Struct({ sessionId: SessionId }), Schema.Struct({ revoked: Schema.Literal(true), deleted: Schema.Literal(true), backupPolicy: Schema.Literal("journal rows and enrollment are omitted from every broker snapshot") }), { exposure: "internal", transaction: "transaction" }),
  acceptTask: command("acceptTask", "runner", "acceptTask", "Admit approved work after broker fingerprint, repository, SHA and capacity checks.", Work, Acceptance, { exposure: "internal" }),
  journalTask: command("journal", "runner", "journalTask", "Reconcile durable runner acceptance without treating pending acceptance as rejection.", TaskKey, Acceptance, { exposure: "internal" }),
  inspectTask: command("inspect", "runner", "inspectTask", "Read capped privacy-projected child events and explicit retention gaps.", RunnerInspect, RunnerInspection, { exposure: "internal" }),
  commandTask: command("command", "runner", "commandTask", "Perform broker-authorized child control and hold aborted work pending host outcome.", RunnerCommand, AgentResponse, { exposure: "internal" }),
  prepareTask: command("prepare", "runner", "prepareTask", "Correct an unprompted child's address after reading actual agent state.", RunnerPrepare, Address, { exposure: "internal" }),
  releaseTask: command("release", "runner", "releaseTask", "Durably record prompt intent then deliver exact confirmed work; never replay uncertain sends.", RunnerRelease, RunnerReleased, { exposure: "internal" }),
  pluginParticipantConsent: command("plugin.participant-consent", "plugin", "pluginParticipantConsent", "Ask agent UI consent before enrolling or revoking this participant.", Schema.Struct({ owner: Peer.fields.owner, sessionId: SessionId, enabled: Schema.Boolean }), Schema.Struct({ participant: Peer.fields.sender, enabled: Schema.Boolean }), { exposure: "local", approval: "participant-consent" }),
  pluginConsent: command("plugin.consent", "plugin", "pluginConsent", "Ask human consent, enroll the active session remotely and persist local journal consent.", Schema.Struct({ participants: ArchiveParticipants.check(Schema.isBetweenLength(1, 32)) }), Schema.Struct({ enabled: Schema.Literal(true) }), { exposure: "local", approval: "archive-consent" }),
  pluginOptout: command("plugin.optout", "plugin", "pluginOptout", "Revoke remote consent before erasing the local journal and deleting server archive rows.", empty, Schema.Struct({ enabled: Schema.Literal(false) }), { exposure: "local" }),
  pluginOff: command("plugin.off", "plugin", "pluginOff", "Persistently stop coordination and archive sharing without aborting accepted agent work; retry off explicitly if remote cleanup fails.", empty, Schema.Struct({ enabled: Schema.Literal(false), sharing: Schema.Literal(false) }), { exposure: "local" }),
  pluginOn: command("plugin.on", "plugin", "pluginOn", "Explicitly resume coordination after completed opt-out cleanup; archive sharing still requires separate consent.", empty, Schema.Struct({ enabled: Schema.Literal(true) }), { exposure: "local" }),
  pluginAccept: command("plugin.accept", "plugin", "pluginAccept", "Accept an unexpired offer using the active agent session identity.", TaskKey, Task, { exposure: "local" }),
  pluginDecline: command("plugin.decline", "plugin", "pluginDecline", "Decline an unexpired offer using the active agent session identity.", TaskKey, Task, { exposure: "local" }),
  pluginSubmit: command("plugin.submit", "plugin", "pluginSubmit", "Ask UI approval of exact work before submitting it to the broker.", Work, Task, { exposure: "local", approval: "exact-work" }),
  pluginReadback: command("plugin.readback", "plugin", "pluginReadback", "Verify active agent identity and actual local repo/ref/SHA before reporting readiness.", pluginReadback, Task, { exposure: "local" }),
  pluginHold: command("plugin.hold", "plugin", "pluginHold", "Report a reasoned hold with next check using the bound agent identity.", pluginReadback, Task, { exposure: "local" }),
  pluginClarify: command("plugin.clarify", "plugin", "pluginClarify", "Request clarification with a reason from the bound agent identity.", pluginReadback, Task, { exposure: "local" }),
  pluginUnable: command("plugin.unable", "plugin", "pluginUnable", "Report inability with a reason from the bound agent identity.", pluginReadback, Task, { exposure: "local" }),
  pluginAddress: command("plugin.address", "plugin", "pluginAddress", "Bind an approved task address through broker ownership and revision fences.", RunnerPrepare, Task, { exposure: "local" }),
  pluginConfirm: command("plugin.confirm", "plugin", "pluginConfirm", "Explicitly confirm matching ready readback to release approved work.", Schema.Struct({ ...TaskKey.fields, revision: Schema.Int }), Task, { exposure: "local", approval: "release" }),
  pluginHandoff: command("plugin.handoff", "plugin", "pluginHandoff", "Request safe ownership handoff to a verified new agent recipient.", Schema.Struct({ ...TaskKey.fields, oldSessionId: SessionId, oldIncarnation: RunnerTask.fields.incarnation, newSessionId: SessionId, newIncarnation: RunnerTask.fields.incarnation, revision: Schema.Int }), Task, { exposure: "local", approval: "handoff" }),
  pluginInstruction: command("plugin.instruction", "plugin", "pluginInstruction", "Ask UI approval of the exact later instruction before recording it.", Schema.Struct({ ...RunnerPrepare.fields, prompt: Text, approved: Schema.Literal(true) }), Task, { exposure: "local", approval: "exact-instruction" }),
  pluginReport: command("plugin.report", "plugin", "pluginReport", "Report task state with session and incarnation injected from the active host.", Schema.Struct({ ...TaskKey.fields, state: ReportState, result: JsonValue }), Task, { exposure: "local" }),
  pluginEvidenceTarget: command("plugin.evidence-target", "plugin", "pluginEvidenceTarget", "Set an observation target without authorizing execution.", Schema.Struct({ ...TaskKey.fields, sha: Evidence.fields.sha, ref: Text, requiredLanes: strings.check(Schema.isBetweenLength(1, 32)) }), Schema.Struct({ target: EvidenceTarget, executionAuthorized: Schema.Literal(false) }), { exposure: "local" }),
  pluginEvidenceCi: command("plugin.evidence-ci", "plugin", "pluginEvidenceCi", "Collect authorized CI observations for the selected task target.", TaskKey, CiEvidence, { exposure: "local" }),
  pluginEvidenceLocal: command("plugin.evidence-local", "plugin", "pluginEvidenceLocal", "Record caller-asserted local check evidence, not a check execution.", LocalEvidence, recorded, { exposure: "local" }),
  pluginEvidenceReview: command("plugin.evidence-review", "plugin", "pluginEvidenceReview", "Record a decision using the enrolled reviewer identity.", ReviewEvidence, recorded, { exposure: "local" }),
  pluginEvidenceDeployment: command("plugin.evidence-deployment", "plugin", "pluginEvidenceDeployment", "Record a caller-supplied deployment observation.", DeploymentEvidence, recorded, { exposure: "local" }),
  pluginStatus: command("plugin.status", "plugin", "pluginStatus", "Read an authorized task and latest evidence.", TaskKey, Task, { exposure: "local" }),
  pluginAttach: command("plugin.attach", "plugin", "pluginAttach", "Read authorized projected runner events.", RunnerInspect, RunnerInspection, { exposure: "local" }),
  pluginControl: command("plugin.control", "plugin", "pluginControl", "Request child control; get_messages is sensitive and abort needs human approval.", RunnerCommand, AgentResponse, { exposure: "local", approval: "control" }),
  pluginArchive: command("plugin.archive", "plugin", "pluginArchive", "Read sanitized archive rows with the active session injected.", Schema.Struct({ cursor: Schema.optional(Schema.Int) }), Schema.mutable(Schema.Array(ArchiveRow)), { exposure: "local" }),
  pluginInbox: command("plugin.inbox", "plugin", "pluginInbox", "Read or explicitly clear the active session's durable local inbox.", Schema.Struct({ clear: Schema.optional(Schema.Boolean) }), Schema.Union([Inbox, Schema.Struct({ cleared: Schema.Literal(true) })]), { exposure: "local" }),
  pluginSessions: command("plugin.sessions", "plugin", "pluginSessions", "List all authorized Coordinator sessions across machines, including stale participants.", empty, sessionList, { exposure: "local" }),
  pluginIdentity: command("plugin.identity", "plugin", "pluginIdentity", "Read actual agent session, worker and local repository identity.", empty, AgentIdentity, { exposure: "local" }),
};
export type Command = typeof Commands[keyof typeof Commands];
export type CommandName = Command["name"];
export type CommandDescriptor<I extends Schema.ConstraintCodec<unknown, unknown>, O extends Schema.ConstraintCodec<unknown, unknown>> =
  {
    readonly name: string;
    readonly target: CommandTarget;
    readonly capability: string;
    readonly exposure: CommandExposure;
    readonly input: I;
    readonly output: O;
    readonly structuredOutput: Schema.Struct<{ readonly result: O }>;
  };
export type CommandInput<D extends { readonly input: Schema.ConstraintCodec<unknown, unknown> }> = D["input"]["Type"];
export type CommandOutput<D extends { readonly output: Schema.ConstraintCodec<unknown, unknown> }> = D["output"]["Type"];
export type CommandCapabilities<T extends CommandTarget, E, R = never> = {
  [D in Extract<Command, { readonly target: T }> as D["capability"]]: (params: CommandInput<D>) => Effect.Effect<CommandOutput<D>, E, R>
};
export const BrokerRequest = Schema.Union(
  Object.values(Commands).filter(descriptor => descriptor.target === "broker").map(descriptor => descriptor.request),
).pipe(Schema.toTaggedUnion("method"));
export type BrokerRequest = typeof BrokerRequest.Type;
export const RunnerRequest = Schema.Union(
  Object.values(Commands).filter(descriptor => descriptor.target === "runner").map(descriptor => descriptor.request),
).pipe(Schema.toTaggedUnion("method"));
export type RunnerRequest = typeof RunnerRequest.Type;
export const McpResult = Schema.Struct({
  result: Schema.Union(Object.values(Commands)
    .filter(descriptor => descriptor.target === "broker" || descriptor.target === "runner" || descriptor.target === "shared")
    .map(descriptor => descriptor.output)),
});
export type McpResult = typeof McpResult.Type;
export const McpToolName = Schema.Literals([...BrokerRequest.discriminants, ...RunnerRequest.discriminants]);
export type McpToolName = typeof McpToolName.Type;
export const McpCommandName = Schema.Union([McpToolName, Commands.health.request.fields.method]);
export type McpCommandName = typeof McpCommandName.Type;
export const BoundaryError = Schema.Struct({ error: Schema.String });
export type BoundaryError = typeof BoundaryError.Type;
export type Text = typeof Text.Type;
export type Repo = typeof Repo.Type;
export type ReportState = typeof ReportState.Type;
export type Disposition = typeof Disposition.Type;
export type ReconciledRunner = typeof ReconciledRunner.Type;
export type Role = typeof Role.Type;
export type ConnectHosts = typeof ConnectHosts.Type;
export type Inbox = typeof Inbox.Type;
export type Job = typeof Job.Type;
export type CiLane = typeof CiLane.Type;
export type AgentResponse = typeof AgentResponse.Type;

export type McpEndpoint<AE, AR, HE, HR> = {
  readonly target: "broker";
  readonly authorize: (token: string) => Effect.Effect<Principal, AE, AR>;
  readonly handle: (principal: Principal, request: BrokerRequest) => Effect.Effect<CommandOutput<Extract<Command, { readonly target: "broker" }>>, HE, HR>;
} | {
  readonly target: "runner";
  readonly authorize: (token: string) => Effect.Effect<string, AE, AR>;
  readonly handle: (principal: string, request: RunnerRequest) => Effect.Effect<CommandOutput<Extract<Command, { readonly target: "runner" }>>, HE, HR>;
};

export class HttpError extends Schema.TaggedError<HttpError>()("HttpError", {
  status: Schema.Int, message: Schema.String,
  retryAfter: Schema.optional(Schema.String.check(Schema.isMaxLength(512))),
  wwwAuthenticate: Schema.optional(Schema.String.check(Schema.isMaxLength(512))),
  protocolCode: Schema.optional(Schema.Int),
}) {}
export class CommandError extends Schema.TaggedError<CommandError>()("CommandError", { message: Schema.String, reason: Schema.optional(Schema.Literals(["transport", "protocol"])) }) {}
