import test from "node:test";
import assert from "node:assert/strict";
import { Schema } from "effect";
import * as C from "../src/contracts.ts";
import { NativeSubmissionId, NativeRunId, NativeBinding, NativeEventSequence } from "@samuelho/coordinator-native-control/contracts";

type Domains = {
  task: C.TaskId; message: C.MessageId; session: C.SessionId; incarnation: C.IncarnationId;
  machine: C.MachineId; worker: C.WorkerId; runner: C.RunnerId; principal: C.PrincipalId;
  owner: C.OwnerId; archive: C.ArchiveEntryId; nativeSubmission: NativeSubmissionId; nativeRun: NativeRunId;
  handled: C.JournalHandledKey; release: C.JournalReleaseKey;
  repository: C.GitHubRepositoryId; run: C.GitHubRunId; workflow: C.GitHubWorkflowId;
  job: C.GitHubJobId; pullRequest: C.GitHubPullRequestNumber; user: C.UserId;
  process: C.ProcessId; sha: C.Sha; hash: C.Hash; repo: C.Repo; snapshot: C.SnapshotKey;
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

type BrokerParams<M extends C.BrokerRequest["method"]> = Extract<C.BrokerRequest, { method: M }>["params"];
type RunnerParams<M extends C.RunnerRequest["method"]> = Extract<C.RunnerRequest, { method: M }>["params"];
type ArchiveMetadata = Extract<C.ArchivePayload, { contentOmitted: true }>;
type ContractFields = {
  task: [C.Task["taskId"], C.TaskId]; owner: [C.Task["owner"], C.OwnerId];
  principal: [C.Principal["id"], C.PrincipalId]; principalOwner: [C.Principal["owner"], C.OwnerId];
  runner: [NonNullable<C.Task["runnerId"]>, C.RunnerId];
  peer: [NonNullable<C.Task["peerSession"]>, C.SessionId];
  message: [C.Address["messageId"], C.MessageId]; session: [C.Address["sessionId"], C.SessionId];
  incarnation: [C.Address["incarnation"], C.IncarnationId]; worker: [C.Address["workerId"], C.WorkerId];
  workTask: [C.Work["taskId"], C.TaskId]; workWorker: [C.Work["workerId"], C.WorkerId];
  workRepository: [C.Work["githubRepoId"], C.GitHubRepositoryId];
  machine: [C.Session["machineId"], C.MachineId]; nativeWorker: [C.NativeIdentity["workerId"], C.WorkerId];
  archive: [C.ArchiveEntry["entryId"], C.ArchiveEntryId];
  archiveId: [ArchiveMetadata["id"], C.ArchiveEntryId]; parent: [NonNullable<ArchiveMetadata["parentId"]>, C.ArchiveEntryId];
  participant: [C.JournalState["participants"][number], C.PrincipalId];
  handled: [keyof C.JournalState["handled"], C.JournalHandledKey];
  release: [keyof C.JournalState["releases"], C.JournalReleaseKey];
  inbox: [keyof C.Inbox, C.MessageId]; seenHash: [C.JournalState["seen"][C.Hash], C.Hash];
  seenKey: [keyof C.JournalState["seen"], C.Hash];
  prompt: [NonNullable<C.RunnerRow["promptId"]>, NativeSubmissionId];
  nativeRun: [NonNullable<C.RunnerRow["nativeRunId"]>, NativeRunId];
  nativeCursor: [C.RunnerRow["nativeCursor"], NativeEventSequence];
  eventBinding: [NonNullable<C.RunnerEvent["binding"]>, NativeBinding];
  eventRun: [NonNullable<C.RunnerEvent["runId"]>, NativeRunId];
  process: [NonNullable<C.RunnerRow["pid"]>, C.ProcessId]; user: [C.RunnerRow["childUid"], C.UserId];
  repository: [C.RepoIdentity["id"], C.GitHubRepositoryId];
  configRepository: [C.BrokerConfig["repositoryIds"][C.Repo], C.GitHubRepositoryId];
  evidenceRepository: [C.CiEvidence["repositoryId"], C.GitHubRepositoryId];
  run: [C.Run["id"], C.GitHubRunId]; workflow: [C.Run["workflow_id"], C.GitHubWorkflowId];
  job: [C.Job["id"], C.GitHubJobId];
  pullRequest: [NonNullable<C.Run["pull_requests"]>[number]["number"], C.GitHubPullRequestNumber];
  brokerTask: [BrokerParams<"task.submit">["taskId"], C.TaskId];
  brokerMessage: [BrokerParams<"peer.ack">["messageId"], C.MessageId];
  brokerSession: [BrokerParams<"peer.ack">["sessionId"], C.SessionId];
  runnerTask: [RunnerParams<"prepare">["taskId"], C.TaskId];
  runnerAddress: [RunnerParams<"prepare">["address"]["sessionId"], C.SessionId];
};
type ContractIdentityLeaks = {
  [K in keyof ContractFields]: [ContractFields[K][0]] extends [ContractFields[K][1]]
    ? [ContractFields[K][1]] extends [ContractFields[K][0]] ? never : K
    : K
}[keyof ContractFields];
export type ConsumerContractsKeepIdentityBrands = AssertNever<ContractIdentityLeaks>;

const wireWork = {
  taskId: "same-id", repo: "github.com/owner/project", ref: "main", sha: "a".repeat(40), launcher: "omp",
  prompt: "Implement approved change", scope: "src", restrictions: "No network", expectedEvidence: ["unit"], deadline: 1000,
  workerId: "same-id", githubRepoId: 0, requiredLanes: ["unit"], bypassPeer: false, approved: true,
};
const wireAddress = {
  messageId: "same-id", revision: 1, workerId: "same-id", sessionId: "same-id", incarnation: "incarnation-1",
  repo: "github.com/owner/project", ref: "main", sha: "a".repeat(40), scope: "src", restrictions: "No network",
  expectedEvidence: ["unit"], deadline: 1000,
};

// These are storage and external request bytes, not internal unbranded fixtures.
test("persisted task, work and address keep primitive identity bytes across JSON and request boundaries", () => {
  const work = C.Work.make({
    ...wireWork, taskId: C.TaskId.make("same-id"), workerId: C.WorkerId.make("same-id"),
    repo: C.Repo.make("github.com/owner/project"), sha: C.Sha.make("a".repeat(40)),
    launcher: "omp", githubRepoId: C.GitHubRepositoryId.make(0), approved: true,
  });
  const address = C.Address.make({
    ...wireAddress, messageId: C.MessageId.make("same-id"), workerId: C.WorkerId.make("same-id"),
    sessionId: C.SessionId.make("same-id"), incarnation: C.IncarnationId.make("incarnation-1"),
    repo: C.Repo.make("github.com/owner/project"), sha: C.Sha.make("a".repeat(40)),
  });
  const task = C.Task.make({
    taskId: C.TaskId.make("same-id"), owner: C.OwnerId.make("owner-1"), fingerprint: C.Hash.make("b".repeat(64)),
    request: work, state: "ACCEPTED", runnerId: C.RunnerId.make("runner-1"),
    peerSession: C.SessionId.make("same-id"), peerIncarnation: C.IncarnationId.make("incarnation-1"),
    address, createdAt: 10, updatedAt: 20,
  });
  const wireTask = {
    taskId: "same-id", owner: "owner-1", fingerprint: "b".repeat(64), request: wireWork, state: "ACCEPTED",
    runnerId: "runner-1", peerSession: "same-id", peerIncarnation: "incarnation-1", address: wireAddress,
    createdAt: 10, updatedAt: 20,
  };
  assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(Schema.MutableJson), { onExcessProperty: "error" })(Schema.encodeUnknownSync(Schema.fromJsonString(C.Work), { onExcessProperty: "error" })(work)), wireWork);
  assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(Schema.MutableJson), { onExcessProperty: "error" })(Schema.encodeUnknownSync(Schema.fromJsonString(C.Address), { onExcessProperty: "error" })(address)), wireAddress);
  assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(Schema.MutableJson), { onExcessProperty: "error" })(Schema.encodeUnknownSync(Schema.fromJsonString(C.Task), { onExcessProperty: "error" })(task)), wireTask);
  assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(C.Work), { onExcessProperty: "error" })(Schema.encodeUnknownSync(Schema.fromJsonString(C.Work), { onExcessProperty: "error" })(work)), work);
  assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(C.Address), { onExcessProperty: "error" })(Schema.encodeUnknownSync(Schema.fromJsonString(C.Address), { onExcessProperty: "error" })(address)), address);
  assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(C.Task), { onExcessProperty: "error" })(Schema.encodeUnknownSync(Schema.fromJsonString(C.Task), { onExcessProperty: "error" })(task)), task);
  assert.deepEqual(Schema.decodeUnknownSync(Schema.toCodecJson(C.BrokerRequest), { onExcessProperty: "error" })({ method: "task.submit", params: wireWork }), { method: "task.submit", params: work });
  assert.deepEqual(Schema.decodeUnknownSync(Schema.toCodecJson(C.RunnerRequest), { onExcessProperty: "error" })({ method: "prepare", params: { taskId: "same-id", address: wireAddress } }), {
    method: "prepare", params: { taskId: task.taskId, address },
  });
  assert.deepEqual(Schema.decodeUnknownSync(Schema.toCodecJson(C.BrokerRequest), { onExcessProperty: "error" })({
    method: "peer.ack", params: { messageId: "same-id", sessionId: "same-id", incarnation: "incarnation-1" },
  }), { method: "peer.ack", params: { messageId: address.messageId, sessionId: address.sessionId, incarnation: address.incarnation } });
});

test("archive parent identities survive serialization without exposing content", () => {
  const append = Schema.decodeUnknownSync(Schema.toCodecJson(C.Commands["archive.append"].input), { onExcessProperty: "error" })({
    sessionId: "same-id", entryId: "entry-2", digest: "c".repeat(64), gaps: [],
    payload: { id: "entry-2", parentId: "entry-1", type: "message", role: "assistant", contentOmitted: true },
  });
  assert.deepEqual(Schema.decodeUnknownSync(Schema.fromJsonString(C.Commands["archive.append"].input), { onExcessProperty: "error" })(Schema.encodeUnknownSync(Schema.fromJsonString(C.Commands["archive.append"].input), { onExcessProperty: "error" })(append)), append);
  assert.throws(() => Schema.decodeUnknownSync(Schema.toCodecJson(C.Commands["archive.append"].input), { onExcessProperty: "error" })({
    ...append, payload: { id: "entry-2", parentId: "entry-1", type: "message", contentOmitted: true, text: "private" },
  }));
  assert.throws(() => Schema.decodeUnknownSync(Schema.toCodecJson(C.Commands["archive.append"].input), { onExcessProperty: "error" })({
    ...append, payload: { id: "entry-2", parentId: "invalid/parent", type: "message", contentOmitted: true },
  }));
});

test("identity branding preserves malformed ID rejection and positive integer repository configuration", () => {
  for (const taskId of ["", "invalid/task", "a".repeat(161)]) {
    assert.throws(() => Schema.decodeUnknownSync(Schema.toCodecJson(C.BrokerRequest), { onExcessProperty: "error" })({ method: "task.submit", params: { ...wireWork, taskId } }));
    assert.throws(() => Schema.decodeUnknownSync(Schema.toCodecJson(C.RunnerRequest), { onExcessProperty: "error" })({ method: "prepare", params: { taskId, address: wireAddress } }));
  }
  for (const field of ["messageId", "sessionId", "workerId", "incarnation"]) {
    assert.throws(() => Schema.decodeUnknownSync(C.Address, { onExcessProperty: "error" })({ ...wireAddress, [field]: "invalid/id" }));
  }
  assert.throws(() => Schema.decodeUnknownSync(C.Work, { onExcessProperty: "error" })({ ...wireWork, githubRepoId: 1.5 }));
  const config = Schema.decodeUnknownSync(C.BrokerConfig, { onExcessProperty: "error" })({
    database: ":memory:", host: "127.0.0.1", port: 0, principals: [], backupDirectory: "/tmp/backups",
    archiveRetentionMs: 1000, backupRetentionMs: 1000, repositoryIds: { "github.com/owner/project": 1 },
  });
  for (const repositoryId of [0, -1, 1.5]) {
    assert.throws(() => Schema.decodeUnknownSync(C.BrokerConfig, { onExcessProperty: "error" })({ ...config, repositoryIds: { "github.com/owner/project": repositoryId } }));
  }
});
