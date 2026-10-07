import { Deferred, Effect, Queue, Schema, type Cause, type Scope } from "effect";
import { Tool } from "effect/ai";
import { ChildProcessSpawner } from "effect/process";
import type { Journal } from "./journal.ts";

const identifierPattern = Schema.isPattern(/^[a-zA-Z0-9_.:-]{1,160}$/);
const identifier = Schema.String.check(identifierPattern);
export const TaskId = identifier.pipe(Schema.brand("@samuelho/coordinator/TaskId"));
export type TaskId = typeof TaskId.Type;
export const MessageId = identifier.pipe(Schema.brand("@samuelho/coordinator/MessageId"));
export type MessageId = typeof MessageId.Type;
export const SessionId = identifier.pipe(Schema.brand("@samuelho/coordinator/SessionId"));
export type SessionId = typeof SessionId.Type;
export const IncarnationId = identifier.pipe(Schema.brand("@samuelho/coordinator/IncarnationId"));
export type IncarnationId = typeof IncarnationId.Type;
export const MachineId = identifier.pipe(Schema.brand("@samuelho/coordinator/MachineId"));
export type MachineId = typeof MachineId.Type;
export const WorkerId = identifier.pipe(Schema.brand("@samuelho/coordinator/WorkerId"));
export type WorkerId = typeof WorkerId.Type;
export const RunnerId = identifier.pipe(Schema.brand("@samuelho/coordinator/RunnerId"));
export type RunnerId = typeof RunnerId.Type;
export const PrincipalId = identifier.pipe(Schema.brand("@samuelho/coordinator/PrincipalId"));
export type PrincipalId = typeof PrincipalId.Type;
export const OwnerId = identifier.pipe(Schema.brand("@samuelho/coordinator/OwnerId"));
export type OwnerId = typeof OwnerId.Type;
export const ArchiveEntryId = identifier.pipe(Schema.brand("@samuelho/coordinator/ArchiveEntryId"));
export type ArchiveEntryId = typeof ArchiveEntryId.Type;
export const NativeRequestId = Schema.String.pipe(Schema.brand("@samuelho/coordinator/NativeRequestId"));
export type NativeRequestId = typeof NativeRequestId.Type;
const nativeRequestId = NativeRequestId.check(identifierPattern);
export const JournalHandledKey = Schema.String.pipe(Schema.brand("@samuelho/coordinator/JournalHandledKey"));
export type JournalHandledKey = typeof JournalHandledKey.Type;
export const JournalReleaseKey = Schema.String.pipe(Schema.brand("@samuelho/coordinator/JournalReleaseKey"));
export type JournalReleaseKey = typeof JournalReleaseKey.Type;
export const GitHubRepositoryId = Schema.Int.pipe(Schema.brand("@samuelho/coordinator/GitHubRepositoryId"));
export type GitHubRepositoryId = typeof GitHubRepositoryId.Type;
export const GitHubRunId = Schema.Int.pipe(Schema.brand("@samuelho/coordinator/GitHubRunId"));
export type GitHubRunId = typeof GitHubRunId.Type;
export const GitHubWorkflowId = Schema.Int.pipe(Schema.brand("@samuelho/coordinator/GitHubWorkflowId"));
export type GitHubWorkflowId = typeof GitHubWorkflowId.Type;
export const GitHubJobId = Schema.Int.pipe(Schema.brand("@samuelho/coordinator/GitHubJobId"));
export type GitHubJobId = typeof GitHubJobId.Type;
export const GitHubPullRequestNumber = Schema.Int.pipe(Schema.brand("@samuelho/coordinator/GitHubPullRequestNumber"));
export type GitHubPullRequestNumber = typeof GitHubPullRequestNumber.Type;
export const UserId = Schema.Int.pipe(Schema.brand("@samuelho/coordinator/UserId"));
export type UserId = typeof UserId.Type;
export const ProcessId = Schema.Int.pipe(Schema.fromBrand("ProcessId", ChildProcessSpawner.ProcessId));
export type ProcessId = typeof ProcessId.Type;
export const Text = Schema.String.check(Schema.isMaxLength(16000));
export const Sha = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)).pipe(Schema.brand("@samuelho/coordinator/Sha"));
export const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)).pipe(Schema.brand("@samuelho/coordinator/Hash"));
export const Repo = Schema.String.check(Schema.isPattern(/^[a-z0-9.-]+\/[a-z0-9_.-]+\/[a-z0-9_.-]+$/)).pipe(Schema.brand("@samuelho/coordinator/Repo"));
export const JsonValue = Schema.MutableJson;
export type JsonValue = typeof JsonValue.Type;
export const Json = Schema.Record(Schema.String, Schema.mutableKey(JsonValue));
export type Json = typeof Json.Type;
const strings = Schema.mutable(Schema.Array(Text));
const boundedStrings = strings.check(Schema.isMaxLength(32));
const participants = Schema.mutable(Schema.Array(PrincipalId));
const port = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 65535 }));

export const Address = Schema.Struct({
  messageId: MessageId, revision: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000000 })),
  workerId: WorkerId, sessionId: SessionId, incarnation: IncarnationId, repo: Repo, ref: Text, sha: Sha,
  scope: Text, restrictions: Text, expectedEvidence: boundedStrings, deadline: Schema.Finite,
});
export type Address = typeof Address.Type;
export const Work = Schema.Struct({
  taskId: TaskId, repo: Repo, ref: Text, sha: Sha, launcher: Schema.Literals(["omp", "pi"]),
  prompt: Text, scope: Text, restrictions: Text, expectedEvidence: boundedStrings, deadline: Schema.Finite,
  workerId: WorkerId, githubRepoId: GitHubRepositoryId, requiredLanes: boundedStrings,
  bypassPeer: Schema.Boolean, approved: Schema.Literal(true),
});
export type Work = typeof Work.Type;
export const TaskState = Schema.Literals(["QUEUED", "ASSIGNING", "ACCEPTED", "RUNNING", "SUCCEEDED", "FAILED", "HELD"]);
export type TaskState = typeof TaskState.Type;
export const ReportState = Schema.Literals(["RUNNING", "SUCCEEDED", "FAILED", "HELD"]);
export const Disposition = Schema.Literals(["ready", "unable", "clarify", "hold"]);
export const EvidenceTarget = Schema.Struct({ sha: Sha, ref: Text, repositoryId: GitHubRepositoryId, requiredLanes: strings });
export type EvidenceTarget = typeof EvidenceTarget.Type;
export const Instruction = Schema.Struct({ prompt: Text, address: Schema.mutableKey(Address) });
export type Instruction = typeof Instruction.Type;
export const ReconciledRunner = Schema.Struct({ runnerId: RunnerId, state: Text, observedAt: Schema.Finite });
export const EvidenceObservation = Schema.Struct({ kind: Text, sha: Sha, principal: PrincipalId, data: JsonValue, observedAt: Schema.Finite, stale: Schema.Boolean });
export type EvidenceObservation = typeof EvidenceObservation.Type;
export const Task = Schema.Struct({
  taskId: TaskId, owner: OwnerId, fingerprint: Hash, request: Work, state: Schema.mutableKey(TaskState),
  runnerId: Schema.mutableKey(Schema.optional(RunnerId)), peerSession: Schema.mutableKey(Schema.optional(SessionId)), peerIncarnation: Schema.mutableKey(Schema.optional(IncarnationId)),
  offerExpires: Schema.mutableKey(Schema.optional(Schema.Finite)),
  offerDecision: Schema.mutableKey(Schema.optional(Schema.Literals(["pending", "accepted", "declined", "expired", "bypass"]))),
  address: Schema.mutableKey(Schema.optional(Address)), readback: Schema.mutableKey(Schema.optional(Address)), confirmedRevision: Schema.mutableKey(Schema.optional(Schema.Int)),
  disposition: Schema.mutableKey(Schema.optional(Disposition)), reason: Schema.mutableKey(Schema.optional(Text)), nextCheck: Schema.mutableKey(Schema.optional(Schema.Finite)),
  result: Schema.mutableKey(Schema.optional(JsonValue)), createdAt: Schema.Finite, updatedAt: Schema.mutableKey(Schema.Finite),
  evidence: Schema.mutableKey(Schema.optional(Schema.mutable(Schema.Array(EvidenceObservation)))),
  evidenceTarget: Schema.mutableKey(Schema.optional(EvidenceTarget)), instruction: Schema.mutableKey(Schema.optional(Instruction)), reconciledRunner: Schema.mutableKey(Schema.optional(ReconciledRunner)),
});
export type Task = typeof Task.Type;
export const Acceptance = Schema.Struct({ taskId: TaskId, accepted: Schema.Boolean, state: Schema.optional(Text), worktree: Schema.optional(Text), address: Schema.optional(Address), reason: Schema.optional(Text) });
export type Acceptance = typeof Acceptance.Type;
export const Role = Schema.Literals(["coordinator", "session", "runner", "archive-writer", "reviewer", "admin"]);
export const Principal = Schema.Struct({ id: PrincipalId, owner: OwnerId, tokenHash: Hash, roles: Schema.Array(Role), repos: Schema.Array(Repo), runnerUrl: Schema.optional(Text), controlTokenHash: Schema.optional(Hash) });
export type Principal = typeof Principal.Type;
export const ObjectBackupConfig = Schema.Struct({ endpoint: Text, region: identifier, bucket: Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/)), accessKeyFile: Text, secretKeyFile: Text });
export type ObjectBackupConfig = typeof ObjectBackupConfig.Type;
const HttpsObjectBackupConfig = Schema.Struct({
  ...ObjectBackupConfig.fields,
  endpoint: Text.check(Schema.isPattern(/^https:\/\/[^/?#@\\\s]+\/?$/i), Schema.makeFilter((value) => URL.canParse(value) ? undefined : "Valid HTTPS object backup authority required")),
});
export const SnapshotKey = Schema.String.check(Schema.isPattern(/^sanitized\/[0-9]{13}\.sqlite$/)).pipe(Schema.brand("@samuelho/coordinator/SnapshotKey"));
export type SnapshotKey = typeof SnapshotKey.Type;
export const SnapshotList = Schema.Struct({ keys: Schema.Array(SnapshotKey), next: Schema.optional(Schema.String.check(Schema.isPattern(/^[A-Za-z0-9+/=._:-]{1,4096}$/))) });
export type SnapshotList = typeof SnapshotList.Type;
export const BrokerConfig = Schema.Struct({ database: Text, host: Text, port, principals: Schema.mutableKey(Schema.Array(Principal)), backupDirectory: Text, archiveRetentionMs: Schema.Finite, backupRetentionMs: Schema.Finite, objectBackup: Schema.optional(HttpsObjectBackupConfig), githubTokenEnv: Schema.optional(Text), repositoryIds: Schema.Record(Repo, GitHubRepositoryId.check(Schema.isGreaterThan(0))) });
export type BrokerConfig = typeof BrokerConfig.Type;
export const RunnerConfig = Schema.Struct({
  id: RunnerId, brokerUrl: Schema.String, tokenFile: Schema.String, host: Schema.String, port, controlTokenHash: Hash,
  stateDirectory: Schema.String, slots: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
  repositories: Schema.Record(Schema.String.pipe(Schema.brand("@samuelho/coordinator/Repo")), Schema.String), launchers: Schema.Record(Schema.String, Schema.String),
  modelDirectory: Schema.String, childDirectory: Schema.String,
  childUidBase: UserId.check(Schema.isBetween({ minimum: 1000000, maximum: 0xffffffff - 1000000 })),
});
export type RunnerConfig = typeof RunnerConfig.Type;
export const PluginConfig = Schema.Struct({ brokerUrl: Schema.String, tokenFile: Schema.String, machineId: MachineId, stateDirectory: Schema.String, selectedRemote: Schema.optional(Schema.String), host: Schema.Literals(["omp", "pi"]) });
export type PluginConfig = typeof PluginConfig.Type;
export const ConnectHosts = Schema.Record(Schema.String, Schema.String);

export const Session = Schema.Struct({ sessionId: SessionId, incarnation: IncarnationId, machineId: MachineId, host: Schema.Literals(["pi", "omp"]), repo: Repo, workerId: Schema.optional(WorkerId) });
export type Session = typeof Session.Type;
export const SessionKey = Schema.Struct({ sessionId: SessionId, incarnation: IncarnationId });
export type SessionKey = typeof SessionKey.Type;
export const TaskKey = Schema.Struct({ taskId: TaskId });
export type TaskKey = typeof TaskKey.Type;
export const LiveSession = Schema.Struct({ ...Session.fields, owner: OwnerId, principal: PrincipalId, verified: Schema.mutableKey(Schema.Boolean), liveUntil: Schema.mutableKey(Schema.Finite), polledAt: Schema.mutableKey(Schema.Finite) });
export type LiveSession = typeof LiveSession.Type;
export const LiveRunner = Schema.Struct({ runnerId: RunnerId, owner: OwnerId, slots: Schema.Int, load: Schema.mutableKey(Schema.Int), liveUntil: Schema.Finite });
export type LiveRunner = typeof LiveRunner.Type;
export const Delivery = Schema.Struct({ messageId: MessageId, incarnation: IncarnationId, payload: JsonValue, expiresAt: Schema.Finite });
export type Delivery = typeof Delivery.Type;
export const Poll = Schema.Struct({ messages: Schema.mutable(Schema.Array(Delivery)), offers: Schema.mutable(Schema.Array(Task)), releases: Schema.mutable(Schema.Array(Task)), archiveEnabled: Schema.optional(Schema.Boolean) });
export type Poll = typeof Poll.Type;

// Only privacy-projected metadata is archive content; peer/native control payloads are separate JSON.
export const ArchivePayload = Schema.Union([
  Schema.Struct({ id: ArchiveEntryId, parentId: Schema.NullOr(ArchiveEntryId), type: Schema.String.check(Schema.isPattern(/^[a-z_]{1,40}$/)), role: Schema.optional(Schema.Literals(["user", "assistant", "toolResult"])), contentOmitted: Schema.Literal(true) }),
  Schema.Struct({ coverageOnly: Schema.Literal(true), gaps: boundedStrings }),
]);
export type ArchivePayload = typeof ArchivePayload.Type;
export const ArchiveEntry = Schema.Struct({ entryId: ArchiveEntryId, digest: Hash, payload: ArchivePayload, gaps: strings });
export type ArchiveEntry = typeof ArchiveEntry.Type;
export const ArchiveRow = Schema.Struct({ ...ArchiveEntry.fields, sequence: Schema.Int, createdAt: Schema.Finite });
export type ArchiveRow = typeof ArchiveRow.Type;
export const InboxEntry = Schema.Struct({ payload: JsonValue, digest: Hash, receivedAt: Schema.Finite });
export type InboxEntry = typeof InboxEntry.Type;
export const Inbox = Schema.Record(Schema.String.pipe(Schema.brand("@samuelho/coordinator/MessageId")), Schema.mutableKey(InboxEntry));
export const JournalState = Schema.Struct({
  handled: Schema.Record(JournalHandledKey, Schema.mutableKey(Schema.Literal(true))),
  releases: Schema.Record(JournalReleaseKey, Schema.mutableKey(Schema.Literals(["intent", "sent"]))),
  consent: Schema.mutableKey(Schema.Boolean), participants: Schema.mutableKey(participants), seen: Schema.mutableKey(Schema.Record(Schema.String.pipe(Schema.brand("@samuelho/coordinator/Hash")), Schema.mutableKey(Schema.String.pipe(Schema.brand("@samuelho/coordinator/Hash"))))),
  fileDigest: Schema.mutableKey(Schema.optional(Hash)), spool: Schema.mutableKey(Schema.mutable(Schema.Array(ArchiveEntry))), gaps: Schema.mutableKey(strings),
  inbox: Schema.mutableKey(Inbox.pipe(Schema.withDecodingDefaultKey(Effect.sync(() => ({}))))),
});
export type JournalState = typeof JournalState.Type;
export const SafeEntry = Schema.Struct({ id: ArchiveEntryId, payload: ArchivePayload, linked: Schema.Boolean });
export type SafeEntry = typeof SafeEntry.Type;
export const ObservedFile = Schema.Struct({ path: Schema.String, size: Schema.Finite, mtimeMs: Schema.Finite });
export type ObservedFile = typeof ObservedFile.Type;

export const RepoIdentity = Schema.Struct({ id: GitHubRepositoryId, full_name: Text, private: Schema.Boolean });
export const Job = Schema.Struct({ id: GitHubJobId, name: Text, status: Text, conclusion: Schema.NullOr(Text), started_at: Schema.NullOr(Text), completed_at: Schema.NullOr(Text) });
export const Run = Schema.Struct({ id: GitHubRunId, workflow_id: GitHubWorkflowId, name: Schema.NullOr(Text), head_sha: Sha, head_branch: Schema.NullOr(Text), pull_requests: Schema.optional(Schema.Array(Schema.Struct({ number: GitHubPullRequestNumber }))), run_attempt: Schema.Int, status: Text, conclusion: Schema.NullOr(Text), html_url: Text, created_at: Text });
export const MergeRef = Schema.Struct({ ref: Text, object: Schema.Struct({ type: Schema.Literal("commit"), sha: Sha }) });
export const CiLane = Schema.Struct({ lane: Text, runId: Schema.optional(GitHubRunId), attempt: Schema.optional(Schema.Int), status: Schema.optional(Text), conclusion: Schema.optional(Schema.NullOr(Text)), jobs: Schema.optional(Schema.mutable(Schema.Array(Job))) });
export const CiEvidence = Schema.Struct({ sha: Sha, repositoryId: GitHubRepositoryId, ref: Text, observedAt: Text, provenance: Text, verdict: Schema.mutableKey(Schema.Literals(["green", "pending", "failed", "unknown"])), reason: Schema.mutableKey(Schema.optional(Text)), lanes: Schema.mutable(Schema.Array(CiLane)) });
export type CiEvidence = typeof CiEvidence.Type;
export const CiSeen = Schema.Struct({ lane: Text, run: Run, jobs: Schema.mutable(Schema.Array(Job)), green: Schema.Boolean });
export type CiSeen = typeof CiSeen.Type;
export const LocalEvidence = Schema.Struct({ taskId: TaskId, sha: Sha, commands: strings, passed: Schema.Boolean });
export type LocalEvidence = typeof LocalEvidence.Type;
export const DeploymentEvidence = Schema.Struct({ taskId: TaskId, sha: Sha, environment: Text, url: Text, observedState: Text, provenance: Text });
export type DeploymentEvidence = typeof DeploymentEvidence.Type;
export const ReviewEvidence = Schema.Struct({ taskId: TaskId, sha: Sha, decision: Schema.Literals(["approved", "changes-requested"]), note: Text });
export type ReviewEvidence = typeof ReviewEvidence.Type;

export const RunnerRow = Schema.Struct({ taskId: TaskId, childUid: UserId, fingerprint: Hash, work: Schema.String, worktree: Schema.String, state: Schema.String, occupied: Schema.Int, pid: Schema.NullOr(ProcessId), incarnation: IncarnationId, address: Schema.NullOr(Schema.String), promptId: Schema.NullOr(NativeRequestId), reason: Schema.NullOr(Schema.String), result: Schema.NullOr(Schema.String), detached: Schema.Int });
export type RunnerRow = typeof RunnerRow.Type;
export const NativeState = Schema.StructWithRest(Schema.Struct({ sessionId: Schema.String.pipe(Schema.brand("@samuelho/coordinator/SessionId")), sessionFile: Schema.optional(Schema.String), isStreaming: Schema.Boolean, messageCount: Schema.Int, isCompacting: Schema.optional(Schema.Boolean), isSettled: Schema.optional(Schema.Boolean), hasPendingAsyncWork: Schema.optional(Schema.Boolean), pendingMessageCount: Schema.optional(Schema.Int), queuedMessageCount: Schema.optional(Schema.Int) }), [Schema.Record(Schema.String, Schema.UndefinedOr(JsonValue))]);
export type NativeState = typeof NativeState.Type;
export const NativeFrame = Schema.StructWithRest(Schema.Struct({ type: Schema.String, id: Schema.optional(NativeRequestId), command: Schema.optional(Schema.String), success: Schema.optional(Schema.Boolean), data: Schema.optional(JsonValue), method: Schema.optional(Schema.String), message: Schema.optional(JsonValue), title: Schema.optional(Schema.String), notifyType: Schema.optional(Schema.String), status: Schema.optional(Schema.String), sessionSettled: Schema.optional(Schema.Boolean) }), [Schema.Record(Schema.String, Schema.UndefinedOr(JsonValue))]);
export type NativeFrame = typeof NativeFrame.Type;
export const NativeIdentity = Schema.Struct({ sessionId: SessionId, incarnation: IncarnationId, machineId: MachineId, repo: Repo, ref: Text, sha: Sha, workerId: WorkerId, workerIdentitySource: Schema.Literal("native-session") });
export type NativeIdentity = typeof NativeIdentity.Type;
const nativeProcess = Schema.declare<ChildProcessSpawner.ChildProcessHandle>((value): value is ChildProcessSpawner.ChildProcessHandle => typeof value === "object" && value !== null && "pid" in value && "stdin" in value && "stdout" in value && "stderr" in value && "kill" in value);
const closeableScope = Schema.declare<Scope.Closeable>((value): value is Scope.Closeable => typeof value === "object" && value !== null && "~effect/Scope/Closeable" in value);
const processInput = Schema.declare<Queue.Queue<Uint8Array, Cause.Done>>(Queue.isQueue<Uint8Array, Cause.Done>);
export const NativePending = Schema.Struct({ type: Schema.String, response: Schema.declare<Deferred.Deferred<NativeFrame, unknown>>(Deferred.isDeferred<NativeFrame, unknown>) });
export type NativePending = typeof NativePending.Type;
const isNativePending = Schema.is(NativePending);
const isNativeRequestId = Schema.is(NativeRequestId);
const nativePendingMap = Schema.declare<Map<NativeRequestId, NativePending>>((value): value is Map<NativeRequestId, NativePending> => {
  if (!(value instanceof Map)) return false;
  for (const [key, item] of value) if (!isNativeRequestId(key) || !isNativePending(item)) return false;
  return true;
});
export const NativeChild = Schema.Struct({ process: nativeProcess, scope: closeableScope, input: processInput, pending: nativePendingMap, lost: Schema.mutableKey(Schema.Boolean), completing: Schema.mutableKey(Schema.Boolean), started: Schema.mutableKey(Schema.Boolean), failed: Schema.mutableKey(Schema.Boolean) });
export type NativeChild = typeof NativeChild.Type;
export const RunnerAdmission = Schema.Struct({ fingerprint: Hash, response: Schema.declare<Deferred.Deferred<Acceptance, unknown>>(Deferred.isDeferred<Acceptance, unknown>) });
export type RunnerAdmission = typeof RunnerAdmission.Type;
export const RunnerInspect = Schema.Struct({ ...TaskKey.fields, cursor: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))) });
export type RunnerInspect = typeof RunnerInspect.Type;
export const RunnerCommand = Schema.Struct({ ...TaskKey.fields, type: Schema.Literals(["get_state", "get_messages", "abort"]) });
export type RunnerCommand = typeof RunnerCommand.Type;
export const RunnerPrepare = Schema.Struct({ ...TaskKey.fields, address: Address });
export type RunnerPrepare = typeof RunnerPrepare.Type;
export const RunnerRelease = Schema.Struct({ ...RunnerPrepare.fields, prompt: Work.fields.prompt });
export type RunnerRelease = typeof RunnerRelease.Type;
export const RunnerReleased = Schema.Struct({ released: Schema.Literal(true) });
export type RunnerReleased = typeof RunnerReleased.Type;
export const RunnerEvent = Schema.Struct({ type: Schema.String.check(Schema.isPattern(/^[a-z_]{1,40}$/)), contentOmitted: Schema.optional(Schema.Literal(true)), address: Schema.optional(Address), state: Schema.optional(Schema.Literals(["ACCEPTED", "LAUNCHING", "READY", "PROMPT_INTENT", "RUNNING", "SUCCEEDED", "FAILED", "HELD"])), success: Schema.optional(Schema.Boolean), status: Schema.optional(Schema.Literals(["completed", "failed", "aborted", "error"])), command: Schema.optional(Schema.Literals(["get_state", "get_messages", "bash", "prompt", "abort"])), fingerprint: Schema.optional(Hash), digest: Schema.optional(Hash), id: Schema.optional(NativeRequestId), reason: Schema.optional(Text), before: Schema.optional(Schema.Int), seq: Schema.optional(Schema.Int) });
export type RunnerEvent = typeof RunnerEvent.Type;
export const RunnerResult = Schema.NullOr(Schema.Struct({ launched: Schema.optional(Schema.Boolean), hostSettled: Schema.optional(Schema.Boolean), evidenceVerified: Schema.optional(Schema.Boolean), exitCode: Schema.optional(Schema.Int), signal: Schema.optional(Schema.String.check(Schema.isPattern(/^SIG[A-Z0-9]{1,16}$/))) }));
export type RunnerResult = typeof RunnerResult.Type;
export const RunnerInspection = Schema.Struct({ ...Acceptance.fields, events: Schema.mutable(Schema.Array(RunnerEvent)), nextCursor: Schema.Int });
export type RunnerInspection = typeof RunnerInspection.Type;
export const NativeResponse = Schema.Union([NativeFrame, Schema.Struct({ type: Schema.Literal("gap"), reason: Text, command: Schema.optional(Schema.String) })]);

// Pi and omp expose this minimal structural subset; no runtime host dependency.
export const Context = Schema.Struct({
  cwd: Schema.String, hasUI: Schema.Boolean,
  mode: Schema.Literals(["tui", "rpc", "json", "print"]),
  isIdle: Schema.declare<() => boolean>((value): value is () => boolean => typeof value === "function"),
  hasPendingMessages: Schema.declare<() => boolean>((value): value is () => boolean => typeof value === "function"),
  sessionManager: Schema.Struct({
    getSessionId: Schema.declare<() => string>((value): value is () => string => typeof value === "function"),
    getSessionFile: Schema.declare<() => string | undefined>((value): value is () => string | undefined => typeof value === "function"),
  }),
  ui: Schema.Struct({
    notify: Schema.declare<(text: string, level?: "info" | "warning" | "error") => void>((value): value is (text: string, level?: "info" | "warning" | "error") => void => typeof value === "function"),
    confirm: Schema.declare<(title: string, text: string) => Promise<boolean>>((value): value is (title: string, text: string) => Promise<boolean> => typeof value === "function"),
  }),
});
export type Context = typeof Context.Type;
export const NativeEventName = Schema.Literals(["session_start", "session_shutdown", "session_switch", "input", "agent_start", "agent_end", "turn_end", "message_end", "agent_before_settle", "agent_settled", "auto_retry_end", "ui_prompt_start", "ui_prompt_end", "tool_approval_requested", "tool_approval_resolved"]);
export type NativeEventName = typeof NativeEventName.Type;
export const Host = Schema.Struct({
  on: Schema.declare<(event: NativeEventName, handler: (event: unknown, ctx: Context) => Promise<void>) => unknown>((value): value is (event: NativeEventName, handler: (event: unknown, ctx: Context) => Promise<void>) => unknown => typeof value === "function"),
  registerCommand: Schema.declare<(name: string, options: { description: string; handler: (args: string, ctx: Context) => Promise<void> }) => void>((value): value is (name: string, options: { description: string; handler: (args: string, ctx: Context) => Promise<void> }) => void => typeof value === "function"),
  sendUserMessage: Schema.declare<(text: string, options: { deliverAs: "followUp" }) => void>((value): value is (text: string, options: { deliverAs: "followUp" }) => void => typeof value === "function"),
});
export type Host = typeof Host.Type;
export const PluginActive = Schema.Struct({ ctx: Context, sessionId: SessionId, workerId: WorkerId, incarnation: IncarnationId, journal: Schema.declare<Journal["Service"]>((value): value is Journal["Service"] => typeof value === "object" && value !== null && "state" in value && "receive" in value && "save" in value), repo: Schema.mutableKey(Repo), heartbeat: Schema.mutableKey(Schema.Finite) });
export type PluginActive = typeof PluginActive.Type;
export const Observation = Schema.Struct({ operation: Schema.String, observed: JsonValue });
export type Observation = typeof Observation.Type;
export const FramePending = Schema.declare<Deferred.Deferred<NativeFrame, CommandError>>(Deferred.isDeferred<NativeFrame, CommandError>);
export type FramePending = typeof FramePending.Type;
export const InstallCommand = Schema.Struct({ binary: Schema.String, args: Schema.mutable(Schema.Array(Schema.String)) });
export type InstallCommand = typeof InstallCommand.Type;
export const InstallPlan = Schema.Struct({
  dryRun: Schema.Boolean, mode: Schema.Literals(["plugin-only", "runner-only"]),
  service: Schema.optional(Schema.String), launcher: Schema.optional(Schema.String),
  program: strings, operations: Schema.mutable(Schema.Array(InstallCommand)),
  pluginEnvironment: Schema.optional(Schema.Struct({ COORDINATOR_CONFIG: Schema.String })),
  host: Schema.optional(Schema.Literals(["omp", "pi"])), package: Schema.String,
});
export type InstallPlan = typeof InstallPlan.Type;
export const ModelProfile = Schema.Struct({ modelRoles: Schema.Struct({ default: Schema.String }) });
export type ModelProfile = typeof ModelProfile.Type;

const ok = Schema.Struct({ ok: Schema.Literal(true) });
const recorded = Schema.Struct({ recorded: Schema.Literal(true) });
export const Health = Schema.Struct({ status: Schema.Literal("ok") });
export type Health = typeof Health.Type;
export type CommandTarget = "broker" | "runner" | "shared" | "plugin" | "native" | "program" | "development";
export type CommandExposure = "tool" | "internal" | "local" | "private";
type CommandPolicy = {
  readonly exposure: CommandExposure;
  readonly method?: string;
  readonly transaction?: "transaction" | "remote";
  readonly offer?: true;
  readonly approval?: "exact-work" | "exact-instruction" | "release" | "handoff" | "participant-consent" | "archive-consent" | "control";
};
// Capabilities capture the authenticated principal or active host context at composition time.
// Executors never import a service, cast a callback, or discard its error/environment channels.
function command<const T extends CommandTarget, const K extends string, I extends Schema.ConstraintCodec<unknown, unknown>, O extends Schema.ConstraintCodec<unknown, unknown>, const P extends CommandPolicy>(
  target: T, capability: K, description: string, input: I, output: O, policy: P,
) {
  return {
    target, capability, description, input, output, method: policy.method,
    transaction: policy.transaction, offer: policy.offer ?? false, approval: policy.approval,
    ...policy,
    structuredOutput: Schema.Struct({ result: output }),
    execute<E, R>(capabilities: Record<K, (params: I["Type"]) => Effect.Effect<O["Type"], E, R>>, params: I["Type"]) {
      return capabilities[capability](params);
    },
    executeDecoded<E, R, D>(
      capabilities: Record<K, (params: I["Type"]) => Effect.Effect<O["Type"], E, R>>,
      params: unknown,
      decodeInput: (schema: I, value: unknown) => Effect.Effect<I["Type"], D>,
    ) {
      return decodeInput(input, params).pipe(
        Effect.flatMap((decoded) => capabilities[capability](decoded)),
      );
    },
  };
}
const empty = Tool.EmptyParams;
const pluginReadback = Schema.Struct({ ...RunnerPrepare.fields, reason: Schema.optional(Text), nextCheck: Schema.optional(Schema.Finite) });
const absolutePath = Schema.String.check(Schema.isPattern(/^\/[^\r\n\u0000]*$/));
export const Commands = {
  health: command("shared", "health", "Read authenticated service health.", empty, Health, { exposure: "internal" }),
  "session.heartbeat": command("broker", "heartbeatSession", "Renew the verified native session lease without replacing a live incarnation.", Session, Schema.Struct({ liveUntil: Schema.Finite }), { exposure: "internal", transaction: "transaction" }),
  "session.poll": command("broker", "pollSession", "Fetch owner- and incarnation-scoped peer messages, offers and releases.", SessionKey, Poll, { exposure: "internal", transaction: "transaction" }),
  "session.stop": command("broker", "stopSession", "Revoke the bound session and its peer deliveries.", SessionKey, Schema.Struct({ stopped: Schema.Literal(true) }), { exposure: "internal", transaction: "transaction" }),
  "peer.send": command("broker", "sendPeer", "Commit an expiring addressed message to a verified recipient; commitment is not delivery.", Schema.Struct({ messageId: MessageId, ...SessionKey.fields, payload: JsonValue, ttlMs: Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 60000 })) }), Schema.Struct({ delivered: Schema.Literal(false), committed: Schema.Literal(true), messageId: MessageId }), { exposure: "tool", transaction: "transaction" }),
  "peer.ack": command("broker", "acknowledgePeer", "Acknowledge a message for the exact live session incarnation.", Schema.Struct({ ...SessionKey.fields, messageId: MessageId }), Schema.Struct({ handled: Schema.Literal(true) }), { exposure: "internal", transaction: "transaction" }),
  "peer.reply": command("broker", "replyPeer", "Accept or decline an unexpired peer offer for the bound native session.", Schema.Struct({ ...SessionKey.fields, ...TaskKey.fields, decision: Schema.Literals(["accepted", "declined"]) }), Task, { exposure: "internal", transaction: "transaction", offer: true }),
  "task.submit": command("broker", "submitTask", "Record exact human-approved bounded work, preserving idempotent task identity.", Work, Task, { exposure: "tool", transaction: "transaction", offer: true, approval: "exact-work" }),
  "task.get": command("broker", "getTask", "Read an authorized task and current evidence observations, including staleness.", TaskKey, Task, { exposure: "tool", transaction: "transaction" }),
  "task.attach": command("broker", "attachTask", "Read projected runner journal events as the task coordinator.", Schema.Struct({ ...TaskKey.fields, cursor: Schema.optional(Schema.Int) }), RunnerInspection, { exposure: "tool", transaction: "remote" }),
  "task.control": command("broker", "controlTask", "Control the assigned child: get_messages is sensitive and abort requires approval.", RunnerCommand, NativeResponse, { exposure: "tool", transaction: "remote", approval: "control" }),
  "task.instruction": command("broker", "instructTask", "Set an exact human-approved later instruction for an accepted native peer.", Schema.Struct({ ...RunnerPrepare.fields, prompt: Text, approved: Schema.Literal(true) }), Task, { exposure: "tool", transaction: "transaction", approval: "exact-instruction" }),
  "task.address": command("broker", "addressTask", "Bind an increased address revision while preserving approved work and reconciling runner readback.", RunnerPrepare, Task, { exposure: "tool", transaction: "remote" }),
  "task.readback": command("broker", "readbackTask", "Record the bound native peer's actual readiness or reasoned disposition.", Schema.Struct({ ...RunnerPrepare.fields, disposition: Disposition, reason: Schema.optional(Text), nextCheck: Schema.optional(Schema.Finite) }), Task, { exposure: "internal", transaction: "transaction" }),
  "task.confirm": command("broker", "confirmTask", "Explicitly confirm matching ready readback and release the approved instruction before its deadline.", Schema.Struct({ ...TaskKey.fields, revision: Schema.Int }), Task, { exposure: "tool", transaction: "remote", approval: "release" }),
  "task.handoff": command("broker", "handoffTask", "Rebind to a verified native recipient only after old ownership has safely reconciled.", Schema.Struct({ ...TaskKey.fields, oldSessionId: SessionId, oldIncarnation: IncarnationId, newSessionId: SessionId, newIncarnation: IncarnationId, revision: Schema.Int }), Task, { exposure: "tool", transaction: "remote", approval: "handoff" }),
  "task.report": command("broker", "reportTask", "Record a confirmed addressed native peer's state and result without terminal regression.", Schema.Struct({ ...SessionKey.fields, ...TaskKey.fields, state: ReportState, result: JsonValue }), Task, { exposure: "internal", transaction: "transaction" }),
  "runner.heartbeat": command("broker", "heartbeatRunner", "Renew the enrolled runner capacity lease and reconcile its durable journal.", Schema.Struct({ runnerId: RunnerId, slots: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })), load: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100000 })), journal: Schema.Array(Acceptance).check(Schema.isMaxLength(256)) }), ok, { exposure: "internal", transaction: "transaction" }),
  "runner.readback": command("broker", "readbackRunner", "Persist assigned runner readback bound to the actual child session.", Schema.Struct({ runnerId: RunnerId, ...RunnerPrepare.fields }), ok, { exposure: "internal", transaction: "transaction" }),
  "runner.report": command("broker", "reportRunner", "Persist assigned runner state and result without terminal regression.", Schema.Struct({ runnerId: RunnerId, ...TaskKey.fields, state: ReportState, result: JsonValue }), ok, { exposure: "internal", transaction: "transaction" }),
  "evidence.target": command("broker", "targetEvidence", "Set the SHA/ref/CI observation target; this does not authorize execution.", Schema.Struct({ ...TaskKey.fields, sha: Sha, ref: Text, requiredLanes: strings.check(Schema.isBetweenLength(1, 32)) }), Schema.Struct({ target: EvidenceTarget, executionAuthorized: Schema.Literal(false) }), { exposure: "tool", transaction: "transaction" }),
  "evidence.ci": command("broker", "collectCiEvidence", "Collect CI observations for the task's evidence target with enrolled provenance.", TaskKey, CiEvidence, { exposure: "tool", transaction: "remote" }),
  "evidence.local": command("broker", "recordLocalEvidence", "Record caller-asserted commands and results; this operation does not run checks.", LocalEvidence, recorded, { exposure: "tool", transaction: "transaction" }),
  "evidence.deployment": command("broker", "recordDeploymentEvidence", "Record a caller-supplied deployment observation; this operation does not deploy.", DeploymentEvidence, recorded, { exposure: "tool", transaction: "transaction" }),
  "evidence.review": command("broker", "recordReviewEvidence", "Record an enrolled reviewer's decision with trusted reviewer identity.", ReviewEvidence, recorded, { exposure: "tool", transaction: "transaction" }),
  "archive.participantConsent": command("broker", "consentParticipant", "Enroll or revoke the authenticated participant's consent after explicit human consent.", Schema.Struct({ owner: OwnerId, sessionId: SessionId, enabled: Schema.Boolean }), Schema.Struct({ participant: PrincipalId, enabled: Schema.Boolean }), { exposure: "internal", transaction: "transaction", approval: "participant-consent" }),
  "archive.consent": command("broker", "consentArchive", "Set bound session archive enrollment with separately consenting participants.", Schema.Struct({ sessionId: SessionId, participants: participants.check(Schema.isMaxLength(32)), enabled: Schema.Boolean }), Schema.Struct({ enabled: Schema.Boolean }), { exposure: "internal", transaction: "transaction", approval: "archive-consent" }),
  "archive.append": command("broker", "appendArchive", "Append only privacy-projected journal metadata under active consent.", Schema.Struct({ sessionId: SessionId, ...ArchiveEntry.fields, gaps: boundedStrings }), Schema.Struct({ appended: Schema.Literal(true), digest: Hash }), { exposure: "internal", transaction: "transaction" }),
  "archive.list": command("broker", "listArchive", "Read authorized sanitized archive metadata in bounded pages.", Schema.Struct({ sessionId: SessionId, cursor: Schema.optional(Schema.Int) }), Schema.mutable(Schema.Array(ArchiveRow)), { exposure: "tool", transaction: "transaction" }),
  "archive.delete": command("broker", "deleteArchive", "Revoke archive enrollment and delete server rows; local optout additionally erases the journal.", Schema.Struct({ sessionId: SessionId }), Schema.Struct({ revoked: Schema.Literal(true), deleted: Schema.Literal(true), backupPolicy: Schema.Literal("journal rows and enrollment are omitted from every broker snapshot") }), { exposure: "internal", transaction: "transaction" }),
  acceptTask: command("runner", "acceptTask", "Admit approved work after broker fingerprint, repository, SHA and capacity checks.", Work, Acceptance, { exposure: "internal" }),
  journal: command("runner", "journalTask", "Reconcile durable runner acceptance without treating pending acceptance as rejection.", TaskKey, Acceptance, { exposure: "internal" }),
  inspect: command("runner", "inspectTask", "Read capped privacy-projected child events and explicit retention gaps.", RunnerInspect, RunnerInspection, { exposure: "internal" }),
  command: command("runner", "commandTask", "Perform broker-authorized child control and hold aborted work pending host outcome.", RunnerCommand, NativeResponse, { exposure: "internal" }),
  prepare: command("runner", "prepareTask", "Correct an unprompted child's address after reading actual native state.", RunnerPrepare, Address, { exposure: "internal" }),
  release: command("runner", "releaseTask", "Durably record prompt intent then deliver exact confirmed work; never replay uncertain sends.", RunnerRelease, RunnerReleased, { exposure: "internal" }),
  "plugin.participant-consent": command("plugin", "pluginParticipantConsent", "Ask native UI consent before enrolling or revoking this participant.", Schema.Struct({ owner: OwnerId, sessionId: SessionId, enabled: Schema.Boolean }), Schema.Struct({ participant: PrincipalId, enabled: Schema.Boolean }), { exposure: "local", approval: "participant-consent" }),
  "plugin.consent": command("plugin", "pluginConsent", "Ask human consent, enroll the active session remotely and persist local journal consent.", Schema.Struct({ participants: participants.check(Schema.isBetweenLength(1, 32)) }), Schema.Struct({ enabled: Schema.Literal(true) }), { exposure: "local", approval: "archive-consent" }),
  "plugin.optout": command("plugin", "pluginOptout", "Revoke remote consent before erasing the local journal and deleting server archive rows.", empty, Schema.Struct({ enabled: Schema.Literal(false) }), { exposure: "local" }),
  "plugin.accept": command("plugin", "pluginAccept", "Accept an unexpired offer using the active native session identity.", TaskKey, Task, { exposure: "local" }),
  "plugin.decline": command("plugin", "pluginDecline", "Decline an unexpired offer using the active native session identity.", TaskKey, Task, { exposure: "local" }),
  "plugin.submit": command("plugin", "pluginSubmit", "Ask UI approval of exact work before submitting it to the broker.", Work, Task, { exposure: "local", approval: "exact-work" }),
  "plugin.readback": command("plugin", "pluginReadback", "Verify active native identity and actual local repo/ref/SHA before reporting readiness.", pluginReadback, Task, { exposure: "local" }),
  "plugin.hold": command("plugin", "pluginHold", "Report a reasoned hold with next check using the bound native identity.", pluginReadback, Task, { exposure: "local" }),
  "plugin.clarify": command("plugin", "pluginClarify", "Request clarification with a reason from the bound native identity.", pluginReadback, Task, { exposure: "local" }),
  "plugin.unable": command("plugin", "pluginUnable", "Report inability with a reason from the bound native identity.", pluginReadback, Task, { exposure: "local" }),
  "plugin.address": command("plugin", "pluginAddress", "Bind an approved task address through broker ownership and revision fences.", RunnerPrepare, Task, { exposure: "local" }),
  "plugin.confirm": command("plugin", "pluginConfirm", "Explicitly confirm matching ready readback to release approved work.", Schema.Struct({ ...TaskKey.fields, revision: Schema.Int }), Task, { exposure: "local", approval: "release" }),
  "plugin.handoff": command("plugin", "pluginHandoff", "Request safe ownership handoff to a verified new native recipient.", Schema.Struct({ ...TaskKey.fields, oldSessionId: SessionId, oldIncarnation: IncarnationId, newSessionId: SessionId, newIncarnation: IncarnationId, revision: Schema.Int }), Task, { exposure: "local", approval: "handoff" }),
  "plugin.instruction": command("plugin", "pluginInstruction", "Ask UI approval of the exact later instruction before recording it.", Schema.Struct({ ...RunnerPrepare.fields, prompt: Text, approved: Schema.Literal(true) }), Task, { exposure: "local", approval: "exact-instruction" }),
  "plugin.report": command("plugin", "pluginReport", "Report task state with session and incarnation injected from the active host.", Schema.Struct({ ...TaskKey.fields, state: ReportState, result: JsonValue }), Task, { exposure: "local" }),
  "plugin.evidence-target": command("plugin", "pluginEvidenceTarget", "Set an observation target without authorizing execution.", Schema.Struct({ ...TaskKey.fields, sha: Sha, ref: Text, requiredLanes: strings.check(Schema.isBetweenLength(1, 32)) }), Schema.Struct({ target: EvidenceTarget, executionAuthorized: Schema.Literal(false) }), { exposure: "local" }),
  "plugin.evidence-ci": command("plugin", "pluginEvidenceCi", "Collect authorized CI observations for the selected task target.", TaskKey, CiEvidence, { exposure: "local" }),
  "plugin.evidence-local": command("plugin", "pluginEvidenceLocal", "Record caller-asserted local check evidence, not a check execution.", LocalEvidence, recorded, { exposure: "local" }),
  "plugin.evidence-review": command("plugin", "pluginEvidenceReview", "Record a decision using the enrolled reviewer identity.", ReviewEvidence, recorded, { exposure: "local" }),
  "plugin.evidence-deployment": command("plugin", "pluginEvidenceDeployment", "Record a caller-supplied deployment observation.", DeploymentEvidence, recorded, { exposure: "local" }),
  "plugin.status": command("plugin", "pluginStatus", "Read an authorized task and latest evidence.", TaskKey, Task, { exposure: "local" }),
  "plugin.attach": command("plugin", "pluginAttach", "Read authorized projected runner events.", Schema.Struct({ ...TaskKey.fields, cursor: Schema.optional(Schema.Int) }), RunnerInspection, { exposure: "local" }),
  "plugin.control": command("plugin", "pluginControl", "Request child control; get_messages is sensitive and abort needs human approval.", RunnerCommand, NativeResponse, { exposure: "local", approval: "control" }),
  "plugin.archive": command("plugin", "pluginArchive", "Read sanitized archive rows with the active session injected.", Schema.Struct({ cursor: Schema.optional(Schema.Int) }), Schema.mutable(Schema.Array(ArchiveRow)), { exposure: "local" }),
  "plugin.inbox": command("plugin", "pluginInbox", "Read or explicitly clear the active session's durable local inbox.", Schema.Struct({ clear: Schema.optional(Schema.Boolean) }), Schema.Union([Inbox, Schema.Struct({ cleared: Schema.Literal(true) })]), { exposure: "local" }),
  "plugin.identity": command("plugin", "pluginIdentity", "Read actual native session, worker and local repository identity.", empty, NativeIdentity, { exposure: "local" }),
  "native.get_state": command("native", "nativeGetState", "Read private child state for actual readback and completion reconciliation.", Schema.Struct({ id: nativeRequestId, type: Schema.Literal("get_state") }), NativeFrame, { exposure: "private" }),
  "native.get_messages": command("native", "nativeGetMessages", "Read private child messages; response may contain sensitive content.", Schema.Struct({ id: nativeRequestId, type: Schema.Literal("get_messages") }), NativeFrame, { exposure: "private" }),
  "native.bash": command("native", "nativeBash", "Execute internal native-child readback or isolation proof shell commands.", Schema.Struct({ id: nativeRequestId, type: Schema.Literal("bash"), command: Text }), NativeFrame, { exposure: "private" }),
  "native.prompt": command("native", "nativePrompt", "Deliver the private child prompt only after durable approved release intent.", Schema.Struct({ id: nativeRequestId, type: Schema.Literal("prompt"), message: Text }), NativeFrame, { exposure: "private" }),
  "native.abort": command("native", "nativeAbort", "Abort the private native child under runner ownership control.", Schema.Struct({ id: nativeRequestId, type: Schema.Literal("abort") }), NativeFrame, { exposure: "private" }),
  "program.broker": command("program", "runBroker", "Start the broker using COORDINATOR_CONFIG private file and remain serving.", Schema.Struct({ configFile: absolutePath }), Schema.Void, { exposure: "private" }),
  "program.runner": command("program", "runRunner", "Start the isolated runner using COORDINATOR_CONFIG private file and await failure.", Schema.Struct({ configFile: absolutePath }), Schema.Void, { exposure: "private" }),
  "program.install:host": command("program", "installHost", "Install one native plugin or privileged runner; help returns usage, dry-run still validates isolation.", Schema.Struct({
    "--help": Schema.optional(Schema.Boolean).annotate({ description: "Print usage without installation." }),
    "--runtime": Schema.optional(absolutePath).annotate({ description: "Absolute executable runtime path; required unless help." }),
    "--host-binary": Schema.optional(absolutePath).annotate({ description: "Absolute executable omp or pi path; required unless help." }),
    "--plugin-only": Schema.optional(Schema.Boolean).annotate({ description: "Install plugin as a nonroot native user; exactly one install mode." }),
    "--runner-only": Schema.optional(Schema.Boolean).annotate({ description: "Install runner as Linux root; exactly one install mode." }),
    "--host": Schema.optional(Schema.Literals(["omp", "pi"])).annotate({ description: "Native host; required for plugin-only." }),
    "--plugin-config": Schema.optional(absolutePath).annotate({ description: "Absolute mode-0600 plugin configuration; required for plugin-only." }),
    "--runner-config": Schema.optional(absolutePath).annotate({ description: "Absolute mode-0600 runner configuration owned by the dedicated service account; required for runner-only." }),
    "--dry-run": Schema.optional(Schema.Boolean).annotate({ description: "Suppress installation writes but still validate and probe isolation." }),
    "--remote": Schema.optional(Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9_.@][a-zA-Z0-9_.@-]*$/))).annotate({ description: "SSH destination for remote installation; requires remote-root." }),
    "--remote-root": Schema.optional(absolutePath).annotate({ description: "Absolute preinstalled remote repository path." }),
  }).check(Schema.makeFilter((flags) => {
    if (flags["--help"]) return undefined;
    if (!flags["--runtime"] || !flags["--host-binary"]) return "Executable --runtime and --host-binary are required";
    if (Boolean(flags["--plugin-only"]) === Boolean(flags["--runner-only"])) return "Select exactly one of --plugin-only or --runner-only";
    if (flags["--plugin-only"] && (!flags["--host"] || !flags["--plugin-config"])) return "Plugin mode requires --host and --plugin-config";
    if (flags["--runner-only"] && !flags["--runner-config"]) return "Runner mode requires --runner-config";
    if (flags["--remote"] && !flags["--remote-root"]) return "Remote installation requires --remote-root";
    return undefined;
  })), Schema.Union([InstallPlan, Schema.Void]), { exposure: "private" }),
  "program.deploy-init.broker": command("program", "initializeBrokerDeployment", "Initialize private broker enrollment and optional file-based backup credentials from deployment environment.", empty, Schema.Void, { exposure: "private" }),
  "program.deploy-init.runner": command("program", "initializeRunnerDeployment", "Initialize private runner configuration from pinned image and enrollment environment.", empty, Schema.Void, { exposure: "private" }),
  "program.smoke": command("program", "runSmoke", "Run the integration smoke with configured real host and GitHub read credential.", Schema.Struct({ "--runner-isolation-only": Schema.optional(Schema.Boolean).annotate({ description: "Run only the real Linux native-child isolation proof." }) }), Schema.Void, { exposure: "private" }),
  "program.smoke:isolation": command("program", "runIsolationSmoke", "Run only the real Linux setpriv and native-child isolation proof.", empty, Schema.Void, { exposure: "private" }),
  "program.smoke:hosts": command("program", "runHostSmoke", "Exercise synthetic OMP and Pi sessions without a model turn.", empty, Schema.Void, { exposure: "private" }),
  "program.smoke:backup": command("program", "runBackupSmoke", "Exercise real object backup using private proof config restricted to loopback and the proof bucket.", empty, Schema.Void, { exposure: "private" }),
  "development.check": command("development", "checkTypes", "Run the repository TypeScript no-emit check.", empty, Schema.Void, { exposure: "private" }),
  "development.test": command("development", "runTests", "Run repository tests serially with the installed tsx test runner.", empty, Schema.Void, { exposure: "private" }),
};
export type CommandName = keyof typeof Commands;
export type CommandNames<T extends CommandTarget> = {
  [N in CommandName]: typeof Commands[N]["target"] extends T ? N : never
}[CommandName];
export type CommandNamesByExposure<X extends CommandExposure> = {
  [N in CommandName]: typeof Commands[N]["exposure"] extends X ? N : never
}[CommandName];
export type CommandInput<N extends CommandName> = typeof Commands[N]["input"]["Type"];
export type CommandOutput<N extends CommandName> = typeof Commands[N]["output"]["Type"];
export type CommandHandlers<T extends CommandTarget, E = unknown, R = never> = {
  [N in CommandNames<T>]: (params: CommandInput<N>) => Effect.Effect<CommandOutput<N>, E, R>
};
export type CommandCapabilities<T extends CommandTarget, E = unknown, R = never> = {
  [N in CommandNames<T> as typeof Commands[N]["capability"]]: (params: CommandInput<N>) => Effect.Effect<CommandOutput<N>, E, R>
};
function requests<const R extends Record<string, { target: string; input: Schema.Constraint }>, const T extends R[keyof R]["target"]>(routes: R, target: T) {
  type Member = {
    [M in keyof R & string]: R[M]["target"] extends T
      ? Schema.Struct<{ readonly method: Schema.Literal<M>; readonly params: R[M]["input"] }>
      : never
  }[keyof R & string];
  const members = Object.entries(routes)
    .filter(([, descriptor]) => descriptor.target === target)
    .map(([method, descriptor]) => Schema.Struct({ method: Schema.Literal(method), params: descriptor.input }));
  // Object.entries erases key/input correlation; the target filter preserves this schema-map relation.
  return Schema.Union(members as Member[]);
}
export const BrokerRequest = requests(Commands, "broker");
export type BrokerRequest = typeof BrokerRequest.Type;
export const RunnerRequest = requests(Commands, "runner");
export type RunnerRequest = typeof RunnerRequest.Type;
export const McpToolName = Schema.Union([
  ...BrokerRequest.members.map((member) => member.fields.method),
  ...RunnerRequest.members.map((member) => member.fields.method),
]);
export type McpToolName = typeof McpToolName.Type;
export const McpCommandName = Schema.Union([McpToolName, ...requests(Commands, "shared").members.map((member) => member.fields.method)]);
export type McpCommandName = typeof McpCommandName.Type;
export const BoundaryError = Schema.Struct({ error: Schema.String });
export type BoundaryError = typeof BoundaryError.Type;
export type Text = typeof Text.Type;
export type Sha = typeof Sha.Type;
export type Hash = typeof Hash.Type;
export type Repo = typeof Repo.Type;
export type ReportState = typeof ReportState.Type;
export type Disposition = typeof Disposition.Type;
export type ReconciledRunner = typeof ReconciledRunner.Type;
export type Role = typeof Role.Type;
export type ConnectHosts = typeof ConnectHosts.Type;
export type Inbox = typeof Inbox.Type;
export type RepoIdentity = typeof RepoIdentity.Type;
export type Job = typeof Job.Type;
export type Run = typeof Run.Type;
export type MergeRef = typeof MergeRef.Type;
export type CiLane = typeof CiLane.Type;
export type NativeResponse = typeof NativeResponse.Type;

export const BrokerServer = Schema.Struct({
  target: Schema.Literal("broker"),
  authorize: Schema.declare<(token: string) => Effect.Effect<Principal, unknown>>((value): value is (token: string) => Effect.Effect<Principal, unknown> => typeof value === "function"),
  handle: Schema.declare<(principal: Principal, request: BrokerRequest) => Effect.Effect<unknown, unknown>>((value): value is (principal: Principal, request: BrokerRequest) => Effect.Effect<unknown, unknown> => typeof value === "function"),
});
export const RunnerServer = Schema.Struct({
  target: Schema.Literal("runner"),
  authorize: Schema.declare<(token: string) => Effect.Effect<string, unknown>>((value): value is (token: string) => Effect.Effect<string, unknown> => typeof value === "function"),
  handle: Schema.declare<(principal: string, request: RunnerRequest) => Effect.Effect<unknown, unknown>>((value): value is (principal: string, request: RunnerRequest) => Effect.Effect<unknown, unknown> => typeof value === "function"),
});
export const McpEndpoint = Schema.Union([BrokerServer, RunnerServer]);
export type McpEndpoint = typeof McpEndpoint.Type;

export class HttpError extends Schema.TaggedError<HttpError>()("HttpError", { status: Schema.Int, message: Schema.String }) {}
export class CommandError extends Schema.TaggedError<CommandError>()("CommandError", { message: Schema.String }) {}
export class BackupError extends Schema.TaggedError<BackupError>()("BackupError", { message: Schema.String }) {}
export class PluginDeclined extends Schema.TaggedError<PluginDeclined>()("PluginDeclined", { message: Schema.String }) {}
