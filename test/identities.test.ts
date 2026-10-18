import { TaskStateEnum } from "../src/generated/database/enums.ts";
import { it } from "@effect/vitest";
import assert from "node:assert/strict";
import { Schema } from "effect";
import * as C from "../src/contracts.ts";
import { TaskId, PeerId, SessionId, RunnerTask, Peer, type Archive, Evidence } from "../src/generated/database/types.ts";
import { AgentHostEnum, AgentSubmissionId, AgentRunId, AgentBinding, AgentEventSequence } from "../src/agent/protocol.ts";
import type { RunnerRow } from "../src/runner.ts";
import type { JournalHandledKey, JournalReleaseKey, JournalState } from "../src/plugin.ts";

type Domains = {
  task: TaskId; message: PeerId; session: SessionId; incarnation: typeof RunnerTask.fields.incarnation.Type;
  machine: C.MachineId; worker: C.WorkerId; runner: C.RunnerId; principal: typeof Peer.fields.sender.Type;
  owner: typeof Peer.fields.owner.Type; archive: typeof Archive.fields.entry.Type; agentSubmission: AgentSubmissionId; agentRun: AgentRunId;
  handled: JournalHandledKey; release: JournalReleaseKey;
  repository: C.GitHubRepositoryId; run: C.GitHubRunId; workflow: C.GitHubWorkflowId;
  job: C.GitHubJobId; pullRequest: C.GitHubPullRequestNumber; user: C.UserId;
  process: NonNullable<typeof RunnerTask.fields.pid.Type>; sha: typeof Evidence.fields.sha.Type; hash: typeof RunnerTask.fields.fingerprint.Type; repo: C.Repo; snapshot: C.SnapshotKey;
};
type AssertNever<T extends never> = T;
type RawIdentityLeaks = {
  [K in keyof Domains]: string extends Domains[K] ? K : number extends Domains[K] ? K : never
}[keyof Domains];
type CrossDomainLeaks = {
  [K in keyof Domains]: {
    [J in Exclude<keyof Domains, K>]: [Domains[K]] extends [Domains[J]] ? [K, J] : never
  }[Exclude<keyof Domains, K>]
}[keyof Domains];
export type RawIdentitiesAreRejected = AssertNever<RawIdentityLeaks>;
export type AllIdentityDomainsAreDistinct = AssertNever<CrossDomainLeaks>;

type ArchiveMetadata = Extract<C.ArchivePayload, { contentOmitted: true }>;
type ContractFields = {
  task: [C.Task["taskId"], TaskId]; owner: [C.Task["owner"], typeof Peer.fields.owner.Type];
  principal: [C.Principal["id"], typeof Peer.fields.sender.Type]; principalOwner: [C.Principal["owner"], typeof Peer.fields.owner.Type];
  runner: [NonNullable<C.Task["runnerId"]>, C.RunnerId];
  peer: [NonNullable<C.Task["peerSession"]>, SessionId];
  message: [C.Address["messageId"], PeerId]; session: [C.Address["sessionId"], SessionId];
  incarnation: [C.Address["incarnation"], typeof RunnerTask.fields.incarnation.Type]; worker: [C.Address["workerId"], C.WorkerId];
  workTask: [C.Work["taskId"], TaskId]; workWorker: [C.Work["workerId"], C.WorkerId];
  workRepository: [C.Work["githubRepoId"], C.GitHubRepositoryId];
  machine: [C.Session["machineId"], C.MachineId]; agentWorker: [C.AgentIdentity["workerId"], C.WorkerId];
  archive: [C.ArchiveEntry["entryId"], typeof Archive.fields.entry.Type];
  archiveId: [ArchiveMetadata["id"], typeof Archive.fields.entry.Type]; parent: [NonNullable<ArchiveMetadata["parentId"]>, typeof Archive.fields.entry.Type];
  participant: [JournalState["participants"][number], typeof Peer.fields.sender.Type];
  handled: [keyof JournalState["handled"], JournalHandledKey];
  release: [keyof JournalState["releases"], JournalReleaseKey];
  inbox: [keyof C.Inbox, PeerId]; seenHash: [JournalState["seen"][typeof RunnerTask.fields.fingerprint.Type], typeof RunnerTask.fields.fingerprint.Type];
  seenKey: [keyof JournalState["seen"], typeof RunnerTask.fields.fingerprint.Type];
  prompt: [NonNullable<RunnerRow["promptId"]>, AgentSubmissionId];
  agentRun: [NonNullable<RunnerRow["agentRunId"]>, AgentRunId];
  agentCursor: [RunnerRow["agentCursor"], AgentEventSequence];
  eventBinding: [NonNullable<C.RunnerEvent["binding"]>, AgentBinding];
  eventRun: [NonNullable<C.RunnerEvent["runId"]>, AgentRunId];
  process: [NonNullable<RunnerRow["pid"]>, NonNullable<typeof RunnerTask.fields.pid.Type>]; user: [RunnerRow["childUid"], C.UserId];
  configRepository: [C.BrokerConfig["repositoryIds"][C.Repo], C.GitHubRepositoryId];
  evidenceRepository: [C.CiEvidence["repositoryId"], C.GitHubRepositoryId];
  job: [C.Job["id"], C.GitHubJobId];
  brokerTask: [C.CommandInput<typeof C.Commands.submitTask>["taskId"], TaskId];
  brokerMessage: [C.CommandInput<typeof C.Commands.acknowledgePeer>["messageId"], PeerId];
  brokerSession: [C.CommandInput<typeof C.Commands.acknowledgePeer>["sessionId"], SessionId];
  runnerTask: [C.CommandInput<typeof C.Commands.prepareTask>["taskId"], TaskId];
  runnerAddress: [C.CommandInput<typeof C.Commands.prepareTask>["address"]["sessionId"], SessionId];
};
type ContractIdentityLeaks = {
  [K in keyof ContractFields]: [ContractFields[K][0]] extends [ContractFields[K][1]]
    ? [ContractFields[K][1]] extends [ContractFields[K][0]] ? never : K
    : K
}[keyof ContractFields];
export type ConsumerContractsKeepIdentityBrands = AssertNever<ContractIdentityLeaks>;

const wireWork = {
  taskId: "same-id", repo: "github.com/owner/project", ref: "main", sha: "a".repeat(40), launcher: AgentHostEnum.omp,
  prompt: "Implement approved change", scope: "src", restrictions: "No network", expectedEvidence: ["unit"], deadline: 1000,
  workerId: "same-id", githubRepoId: 0, requiredLanes: ["unit"], bypassPeer: false, approved: true,
};
const wireAddress = {
  messageId: "same-id", revision: 1, workerId: "same-id", sessionId: "same-id", incarnation: "incarnation-1",
  repo: "github.com/owner/project", ref: "main", sha: "a".repeat(40), scope: "src", restrictions: "No network",
  expectedEvidence: ["unit"], deadline: 1000,
};

// These are storage and external request bytes, not internal unbranded fixtures.
it("persisted task, work and address keep primitive identity bytes across JSON and request boundaries", () => {
  const work = C.Work.make({
    ...wireWork, taskId: TaskId.make("same-id"), workerId: C.WorkerId.make("same-id"),
    repo: C.Repo.make("github.com/owner/project"), sha: Evidence.fields.sha.make("a".repeat(40)),
    launcher: AgentHostEnum.omp, githubRepoId: C.GitHubRepositoryId.make(0), approved: true,
  });
  const address = C.Address.make({
    ...wireAddress, messageId: PeerId.make("same-id"), workerId: C.WorkerId.make("same-id"),
    sessionId: SessionId.make("same-id"), incarnation: RunnerTask.fields.incarnation.make("incarnation-1"),
    repo: C.Repo.make("github.com/owner/project"), sha: Evidence.fields.sha.make("a".repeat(40)),
  });
  const task = C.Task.make({
    taskId: TaskId.make("same-id"), owner: Peer.fields.owner.make("owner-1"), fingerprint: RunnerTask.fields.fingerprint.make("b".repeat(64)),
    request: work, state: TaskStateEnum.ACCEPTED, runnerId: C.RunnerId.make("runner-1"),
    peerSession: SessionId.make("same-id"), peerIncarnation: RunnerTask.fields.incarnation.make("incarnation-1"),
    address, createdAt: 10, updatedAt: 20,
  });
  const wireTask = {
    taskId: "same-id", owner: "owner-1", fingerprint: "b".repeat(64), request: wireWork, state: TaskStateEnum.ACCEPTED,
    runnerId: "runner-1", peerSession: "same-id", peerIncarnation: "incarnation-1", address: wireAddress,
    createdAt: 10, updatedAt: 20,
  };
  assert.deepEqual(Schema.decodeSync(Schema.fromJsonString(Schema.MutableJson), { onExcessProperty: "error" })(Schema.encodeSync(Schema.fromJsonString(C.Work), { onExcessProperty: "error" })(work)), wireWork);
  assert.deepEqual(Schema.decodeSync(Schema.fromJsonString(Schema.MutableJson), { onExcessProperty: "error" })(Schema.encodeSync(Schema.fromJsonString(C.Address), { onExcessProperty: "error" })(address)), wireAddress);
  assert.deepEqual(Schema.decodeSync(Schema.fromJsonString(Schema.MutableJson), { onExcessProperty: "error" })(Schema.encodeSync(Schema.fromJsonString(C.Task), { onExcessProperty: "error" })(task)), wireTask);
  assert.deepEqual(Schema.decodeSync(Schema.fromJsonString(C.Work), { onExcessProperty: "error" })(Schema.encodeSync(Schema.fromJsonString(C.Work), { onExcessProperty: "error" })(work)), work);
  assert.deepEqual(Schema.decodeSync(Schema.fromJsonString(C.Address), { onExcessProperty: "error" })(Schema.encodeSync(Schema.fromJsonString(C.Address), { onExcessProperty: "error" })(address)), address);
  assert.deepEqual(Schema.decodeSync(Schema.fromJsonString(C.Task), { onExcessProperty: "error" })(Schema.encodeSync(Schema.fromJsonString(C.Task), { onExcessProperty: "error" })(task)), task);
  assert.deepEqual(Schema.decodeUnknownSync(Schema.toCodecJson(C.BrokerRequest), { onExcessProperty: "error" })({ method: C.Commands.submitTask.name, params: wireWork }), { method: C.Commands.submitTask.name, params: work });
  assert.deepEqual(Schema.decodeUnknownSync(Schema.toCodecJson(C.RunnerRequest), { onExcessProperty: "error" })({ method: C.Commands.prepareTask.name, params: { taskId: "same-id", address: wireAddress } }), {
    method: C.Commands.prepareTask.name, params: { taskId: task.taskId, address },
  });
  assert.deepEqual(Schema.decodeUnknownSync(Schema.toCodecJson(C.BrokerRequest), { onExcessProperty: "error" })({
    method: C.Commands.acknowledgePeer.name, params: { messageId: "same-id", sessionId: "same-id", incarnation: "incarnation-1" },
  }), { method: C.Commands.acknowledgePeer.name, params: { messageId: address.messageId, sessionId: address.sessionId, incarnation: address.incarnation } });
});

it("archive parent identities survive serialization without exposing content", () => {
  const append = Schema.decodeUnknownSync(Schema.toCodecJson(C.Commands.appendArchive.input), { onExcessProperty: "error" })({
    sessionId: "same-id", entryId: "entry-2", digest: "c".repeat(64), gaps: [],
    payload: { id: "entry-2", parentId: "entry-1", type: "message", role: "assistant", contentOmitted: true },
  });
  assert.deepEqual(Schema.decodeSync(Schema.fromJsonString(C.Commands.appendArchive.input), { onExcessProperty: "error" })(Schema.encodeSync(Schema.fromJsonString(C.Commands.appendArchive.input), { onExcessProperty: "error" })(append)), append);
  assert.throws(() => Schema.decodeUnknownSync(Schema.toCodecJson(C.Commands.appendArchive.input), { onExcessProperty: "error" })({
    ...append, payload: { id: "entry-2", parentId: "entry-1", type: "message", contentOmitted: true, text: "private" },
  }));
});

it("identity boundaries reject non-string IDs and invalid repository integers", () => {
  assert.throws(() => Schema.decodeUnknownSync(Schema.toCodecJson(C.BrokerRequest), { onExcessProperty: "error" })({
    method: C.Commands.submitTask.name, params: { ...wireWork, taskId: 1 },
  }));
  assert.throws(() => Schema.decodeUnknownSync(C.Work, { onExcessProperty: "error" })({ ...wireWork, githubRepoId: 1.5 }));
  const config = Schema.decodeUnknownSync(C.BrokerConfig, { onExcessProperty: "error" })({
    host: "127.0.0.1", port: 0, principals: [], backupDirectory: "/tmp/backups",
    archiveRetentionMs: 1000, backupRetentionMs: 1000, repositoryIds: { "github.com/owner/project": 1 },
  });
  for (const repositoryId of [0, -1, 1.5]) {
    assert.throws(() => Schema.decodeUnknownSync(C.BrokerConfig, { onExcessProperty: "error" })({ ...config, repositoryIds: { "github.com/owner/project": repositoryId } }));
  }
});
