# Persistent agent orchestration: Rex, Hermes, and Coordinator

Research snapshot: 2026-10-15; selected-direction clarification: 2026-10-16; implementation-status addendum: 2026-10-17. External research/evaluations remain dated evidence, not an approved runtime migration.
Hermes source is pinned to `56808f8deeb8d2e9256a403fddff353f63eacd8d`; original Coordinator research inspected `f5728f3685054d4b31ffc85aef7c850b8949321a`. Numeric local anchors below refer to that **historical baseline**, not current import paths; current owners are mapped in the consolidation report.
Live vendor documentation describes capabilities, not an installed stable build or verified deployment.
Hermes v0.22.2 is a published non-prerelease at commit `d923a5eb487a17c0a68801bba9ef0f56c81040a8`; inspected main is a different revision. Managed-owner/admission features below must not be assumed present in that stable release. [H0, H5]
The original research installed nothing, changed no infrastructure and ran no application gates or proposed integration evaluations. Later parent-observed consolidation proofs and a bounded native-RPC API investigation are separately recorded in the implementation report; they do not establish the human workflow or Rex/Hermes integration.

## 1. Conclusion and corrections

**Selected direction: actual human Pi/OMP sessions plus the Coordinator broker.** Rex is a conditional persistent-terminal/native-profile substrate; Hermes optional MCP client/Rex CLI invoker, not fleet host or second board. [The consolidation report](src-consolidation-plan.md) records **implemented and locally verified source phases1–5**, the full **13 handwritten +5 generated =18-file tree**, all27 baseline dispositions, six application Context owners, real factories/methods/scopes and matching Effect4.0.3 citations. Final checks and the **unfiltered128/128-test suite** passed, with real stock-host, OS-isolation, PostgreSQL-recovery and disposable S3 proofs. A native RPC probe showed passive context insertion before an explicit prompt and later provider-request inclusion, not durable admission/consumption. Physical consolidation **does not bind the human conversation**: ordinary Plugin retains private SDK execution. Native cutover and proposed work/context/designation/CI-link extensions remain blocked on precise receipt/profile/platform prerequisites, not usable features or source placeholders.

**Rex is a terminal substrate candidate, not a demonstrated full coordination authority.** Reuse its persistent terminals, remote attachment, launch/control APIs and status display rather than building another multiplexer. Public testing is presently macOS-only; public Linux server acquisition and supported homelab service deployment remain unresolved. [R1–R5]

**Preserve the native-reuse evidence without recommending a host migration.** Hermes already implements substantial independent-profile/chat, durable Kanban, coordinator, dependency, handoff, notification, peer and exact-head PR completion behavior. Those capabilities remain genuine [H1–H8], but a Hermes-native fleet is an unselected alternative, not a missing-feature claim about Hermes or the current recommendation. Implement only the demonstrated gaps in actual Pi/OMP-session publication/context/control.

**Do not run two competing task brokers.** Coordinator remains the selected authoritative work lifecycle. Optional Hermes consumes its authorized MCP/Rex paths; Hermes Kanban must not become an authoritative mirror of the same tasks.

**Keep CI/CD enforcement native.** GitHub owns PR/review/check/merge policy; trusted pipelines own artifact production; operator Git and Argo own desired/live deployment reconciliation; the migration executor/database owns migration history and serialization. Agents share intent and propose actions, not acquire global mutation authority from a board. [G1–G7, A1–A6, P1]

This recommendation does **not** approve SQLite as Coordinator's project database, automatic merge/deploy/activation, weaker security, or loss of no-replay guarantees. Standing policy remains exact-source CI → verified immutable image/chart → reviewed operator pins → Argo reconciliation → private control-path acceptance. [C1]

## 2. Workflows and identity vocabulary

### Workflow A: designated coordinator queues work

A human starts a designated coordinator agent in a persistent homelab terminal with its native profile.
That coordinator creates/links work, chooses eligible specialists, manages dependencies and blockers, and requests human decisions.
Specialists accept work through the selected authority; completion includes relevant handoff context and external evidence.
The coordinator need not launch every terminal, and reasoning authority is not permission to merge or deploy.
Hermes profiles plus Kanban provide genuine native overlap with this workflow [H1–H4]; the selected implementation keeps Pi/OMP native sessions and Coordinator authority, with Rex supplying terminal mechanics where available. [R3–R5]

### Workflow B: humans independently prompt specialists

A human separately opens agent A and agent B, selects their profiles, and prompts them directly.
Each publishes its own relevant intent, progress, blockers and context; the coordinator detects overlap and facilitates sequencing.
Neither specialist must become a child of the coordinator or surrender its human conversation.
Shared visibility must cover planned work **before** PR creation, not only completed commits or green CI badges.
Hermes ordinary profiles can use Kanban, but automatic publication of every human turn is not established; the current Coordinator plugin does not observe the outer TUI turn stream. [H2, H3, C2]

### Terms that must remain distinct

| Term | Meaning and authority |
| --- | --- |
| Human/operator | Person choosing work and approving consequential actions under policy; not inferred from an agent label. |
| Principal | Authenticated credential subject with granted roles/resources; several sessions may use it. |
| Profile | Native agent configuration/persona/skills/memory/credentials namespace; not a unique running conversation or OS sandbox. |
| Terminal/block | Process/display/input container; Rex stable IDs identify this layer, not the model's conversation. |
| Host session | Actual Pi/OMP/Hermes conversation and execution context receiving a human prompt. |
| Owned SDK session | Separate conversation created and managed by an integration; not automatically the surrounding human TUI. |
| Task/card | Durable shared unit of declared work, ownership, dependencies, context and lifecycle. |
| Attempt/run/admission receipt | Particular accepted execution; distinguishes retry from observation and queued from consumed. |
| Coordinator | Selected reasoning/coordination participant; a role grant alone does not select a unique agent. |
| Claim/lease | Cooperative ownership/liveness statement; expiry is not proof external work stopped. |
| Effect authority | Credential/policy-enforced path allowed to merge, publish, migrate or deploy. |
| Evidence | Scoped observation tied to immutable identities and time; not implicit authorization. |

Independent human sessions are therefore different from profiles **and** spawned workers.
One profile can own several sessions; a managed worker may use that profile in a different session from the human's open chat.
A spawned child normally receives parent-selected context and returns a summary; it does not establish discovery of independent human work.
A terminal process surviving client disconnect establishes neither durable task settlement nor survival across host reboot. [H1, H4, H10, R3, R6]

## 3. Availability and actual capability comparison

“Documented” below means first-party docs or pinned code, not exercised acceptance.
No live installed/deployed version, current GitHub governance, or homelab policy was audited.

| Layer | Rex | Hermes | Current Coordinator |
| --- | --- | --- | --- |
| Acquisition/runtime | macOS beta includes local server; Linux/Windows servers reportedly exist but are outside public testing. Runtime account-free promise differs from beta Discord/download access. [R1, R2] | Public source/docs and native CLI profiles; this inspection pins main source, not a verified stable installation. [H0, H1] | README calls beta.5 unpublished/undeployed candidate; not independent deployment evidence. `README.md:74-78`. |
| Persistent identity | Stable session/window/block IDs and persistent terminal processes across client absence; labels mutable/nonunique. [R3] | Separate profile homes with config, credentials, memory, skills, sessions and state; native `hermes -p coder chat`. [H1] | Principal/session/incarnation/machine/host/repo identities; no explicit selected profile identity. `src/contracts.ts:108-149`. |
| Human-originated work | Existing terminal inspection/events do not require coordinator-created terminals; semantic work publication needs agent integration. [R4–R7] | Independent chats and ordinary-profile Kanban tools support deliberate publication; not automatic global transcript sharing. [H1–H3] | Private SDK task lifecycle is reported, not outer human-turn lifecycle. [C2] |
| Durable queue | No reviewed source establishes task leases, dependencies or uncertain execution ownership. | Kanban cards, atomic claims, runs, dependencies, comments, attachments, review and dispatch ownership. [H3–H5] | Durable bounded-task broker, peer accept/decline, runner assignment, task readback/release. [C3] |
| Coordinator reasoning | Can host chosen CLI/profile; Lua is automation, not automatically a durable scheduler. [R4, R5] | Designated orchestrator profile, optional decomposition, tools/skills and wake subscriptions. [H3, H4] | Coordinator role authorizes submissions; no selected coordinator profile/lifecycle. `src/broker.ts:132-147,328-336`. |
| Cross-machine cooperation | Declared remote servers/endpoints and events; deployment/auth/reconnect guarantees unresolved. [R5, R6] | Peer API without Desktop, Desktop relay, A2A, MCP client, external driving APIs. [H6–H9] | Authenticated broker and private host/runner integration; existing security/ownership contracts must be preserved. [C3, C4] |
| Relevant shared context | Screen/process/status capture, not complete conversation or task history. [R4, R7] | Kanban comments/artifacts/parent summaries, messages and optional shared memory provider. [H3, H6, H11] | Task result JSON, peer inbox, consented private-session archive metadata; not general outer-chat sharing. [C2, C3] |
| CI/CD | Status is untrusted display, not release evidence. [R7] | Real exact-head PR completion gate and review workflow; not merge/deployment authority. [H8, H12] | Exact-SHA CI observations with explicit `executionAuthorized: false`; not global CI/CD arbitration. [C5] |

The table's Coordinator numeric anchors are historical. Current paths are `src/agent/protocol.ts` (catalog/lifecycle/pure presentation), `src/agent/server.ts` (transport/invocation/presentation), `src/agent-session.ts` (private SDK/server/isolated entrypoint), `src/plugin.ts` (ordinary integration/journal/status), `src/database.ts` (DB owner/readiness) and `src/broker.ts` (authority/dispatch/GitHub). There are66 shared root descriptors +9 localized program names after local off/on participation controls were added;69 agent descriptors still have only9 actual private handlers. Phase5 shares slots1–64 across RunnerConfig/heartbeat and removes heartbeat journal; durable reports and assign/handoff reconciliation remain. These are implemented source capabilities, not proof of native human work publication.

### Hermes capabilities that materially change the decision

Kanban is a durable task state machine: `triage/todo/ready/running/blocked/review/done/archived`, assignee profiles, idempotency keys, dependency links, comments, attempt history and attachments.
Human CLI, agent tools and dashboard use the lifecycle kernel; coordinator auto-decomposition is optional.
Global/per-profile concurrency caps exist but default unlimited; single-dispatcher ownership and profile-owner notification routing matter operationally. [H3, H4]

Pinned **main** dispatch code launches `hermes_cli.kanban_worker_client`, which submits `kanban.run` to the assigned local profile owner and waits for the exact `prompt.receipt`.
Stable v0.22.2 instead launches `hermes -p … --cli … chat -q …`; the CLI-spawn documentation matches that release, not the inspected main path. Neither path proves arbitrary control of already-running Pi/OMP terminals. [H0, H5]
External CLI worker lanes are explicitly **not yet paved**; workspace, lifecycle, sandbox/auth and per-CLI policy integration remain work. [H5]

Subscriptions distinguish passive notification from waking a reasoning turn, including the originating chat when configured.
Bot-to-bot messages target canonical Bot Chats, not every ordinary CLI session; headless Bot Mode needs explicit metadata/canonical-chat setup.
Cross-connection Desktop relay needs Desktop running; `hermes peer` instead supports direct authenticated peer query/run/status/stop, with durable run identities.
Live Bot Mode docs also describe 2–6-member Groups with persistent member sessions and a same-gateway durable driver that continues after Desktop closes. Later human questions/member answers in that **same room-member session** can automatically project into the room when reopened/driven; this is not observation of arbitrary private chats or Pi/OMP terminals, and stable-release parity was not established. [H2]
A2A supports cross-framework tasks/contexts, streaming, cancellation and notifications, but Pi/OMP interoperability was not established. [H4, H6, H7]

MCP is not only a client story: Hermes has a curated stdio tool server for its Codex app-server runtime.
Its actual exposed tuple includes Kanban read/handoff/orchestrator tools but excludes terminal/file/process and context-dependent delegation/memory/session search.
That is not a general full-session MCP service for OMP.
ACP, TUI gateway JSON-RPC/WebSocket and HTTP run APIs are genuine driving surfaces; a documented Pi-style mapping is not a verified Pi/OMP adapter. [H9]

PR completion contracts inspect protection/rulesets, exact-head checks and legacy statuses, re-read head SHA/base branch **name**, and fail closed on missing, pending, bad, stale or unreadable required evidence.
The real gate exists in stable v0.22.2 as well as inspected main; it accepts OPEN or MERGED PRs, so completion does not prove merge. Unlike GitHub's native eligible-check policy, its classifier treats skipped/neutral conclusions as infrastructure failures rather than success.
It does not revalidate the base commit/integration revision, continuously monitor completion or verify artifacts/deployments; undeclared cards remain local-only. [H8, G3]
GitHub and SDLC-review skills/webhooks support real collaboration, but instructions and event triggers are not hard authorization. [H8, H12]

### Limits of persistence, context and security

Hermes profile memory snapshots are frozen during a conversation; writing memory does not live-broadcast into other running chats.
Use task comments/messages/artifact pointers for fresh relevant context rather than copying whole transcripts or expecting shared omniscience. [H11]
Profiles are not filesystem sandboxes; separate unmanaged processes must not concurrently write the same profile home.
Managed gateway ownership is different from opening another writer against that directory. [H1, H13]
Host subprocess `HOME` defaults to the real OS home, so git/ssh/gh/cloud CLI state can remain shared unless `terminal.home_mode: profile` is selected; even that is not an OS sandbox. PR acceptance's assignee-specific `gh` environment is a narrower isolation measure, and unassigned cards use ambient credentials. [H1, H8]
Unknown admissions can pause reclaim while the bound interpreter may still run; after interpreter death, crash reclaim can repeat work whose external effects already occurred.
Cancellation, exact-run stop, TTL expiry and process death are not rollback or global exactly-once execution. [H3, H9]

Rex OSC 7501 status is explicitly untrusted terminal output: `blocked permission` is not an approval rule and `done` is not current-SHA CI evidence.
Records replace prior fields, have bounded retention/eviction, need no heartbeat and can survive some child-exit events; status ID hierarchy supports clearing/app inheritance/optional grouping, not authenticated agent identity.
Rex APIs can read status even though the OSC protocol itself cannot; mobile push/unified priority inbox remain future plans. [R7]
Lua scripts run where their CLI runs, with ordinary Lua I/O/OS access; serial handlers can block one another.
No inspected Rex source establishes durable event replay, acknowledgment or gap-free reconnect; those remain unknown, not asserted absent forever. [R5, R6]

## 4. Current implementation gaps and retained contracts

The existing broker is substantive, not disposable glue: participant leases, authenticated role/repo grants, durable dispatch, explicit accept/readback/report/release, runner isolation and evidence have value. [C3, C4]
The following gaps are specific to the requested workflow rather than claims that upstream hosts lack features:

- `src/agent-session.ts` deliberately creates an SDK session independent of the surrounding TUI; `src/plugin.ts` enrolls that private binding. Physical consolidation did not change this into human-session observation. [C2]
- Current host Context/Host/AgentEventName types and callbacks live in `src/plugin.ts`, not contracts.ts; start/switch/shutdown/command integration does not publish outer prompts/transcript/turns.
- Model context is not native profile identity. Shared `ModelProfile` remains a real model-role schema consumed by scripts/deploy-init.ts and scripts/smoke.ts; do not delete before verified native Pi/OMP/Rex profile migration. Agent host/model wire schemas now live in `src/agent/protocol.ts`. [C2]
- Coordinator role authorization still does not designate a unique coordinator conversation/process or settle conflicts. Current authority remains Broker.handle in `src/broker.ts`; proposed designation is blocked, not an existing method. [C3]
- Peer inbox UI notification/ack is durable inbox receipt, not native model admission/consumption. Task reporting remains bound to accepted private runs, not arbitrary human work. Pinned Pi1.0.4/OMP18.6.1 public void sends do not provide durable per-input ID admission or cancel/drop/switch/restart/consumption outcomes. Preserve private execution until a safe successor exists; no fabricated receipt/identity, automatic wake or fallback. [C2, C3]
- Worktree/ref/SHA isolation does not lock a shared PR, artifact namespace, database or deployment environment. Existing workflow concurrency is repository workflow policy, not universal broker enforcement. [C5, C6]

The broker dispatch queue and owned-session FIFO are different layers; do not collapse them because both are called queues.
Any replacement must map actual admission, session/incarnation/generation, lifecycle sequencing, whole-run settlement, cancellation, transcript consent and per-client capabilities. [C3, C4]
Preserve uncertain-delivery journals, readback and no-replay fences; a timeout/disconnection must not become permission to resend a consequential action.
Preserve host human approvals rather than bypassing them through an unrelated headless helper session.
PostgreSQL ownership/durability, isolated execution, exact-source artifact trust and reviewed promotion remain acceptance constraints, not optional migration costs. [C1, C4]

## 5. Minimal ownership split and reuse decision

The smallest useful system has four owners, not an agent-built replacement for every platform:

1. **Terminal/agent host:** terminal longevity, native profile selection, actual conversation lifecycle, host approvals and tools. Rex is optional pending availability; existing terminals remain valid.
2. **One coordination lifecycle:** Coordinator broker owns tasks, explicit participant/session binding, advisory intent/claims, dependencies, relevant handoffs, attempt/unknown state and evidence pointers. Hermes Kanban remains an unselected native alternative, not a concurrent authority.
3. **Designated coordinator agent:** reasoning over shared work, discovering overlaps, prioritizing offers, negotiating dependencies and requesting approvals; not omniscient transcript access.
4. **Effect-owning platforms:** enforce PR/review/check rules, trusted publication, migration serialization, reviewed desired-state changes and reconciliation/acceptance.

Minimum shared publication: actual principal/profile/session, task/attempt, repository/worktree/ref/current SHA, affected components/resources, intent versus observed effect, dependencies/blockers, fresh status and relevant context pointers.
Publish only authorized task-relevant context; credentials and unrelated private conversations must not be broadcast.
A claimed resource is advisory until enforced at its effect boundary; stale presence does not authorize taking it over.
Exact schemas, adapters and new locking systems are outside this research.

| Decision | Recommendation and condition |
| --- | --- |
| Reuse now in design | Native profiles, terminal APIs, GitHub rules/checks, immutable artifacts and Argo reconciliation rather than custom equivalents. No installation is approved by this report. |
| Selected future: actual Pi/OMP + Coordinator | Preserve broker authority; actual-human intent/status/context integration is blocked on native per-ID durable admission/outcome/consumption receipts and profile migration. Retain current private SDK execution meanwhile, without calling it human attachment. Rex terminal/profile launch requires supported platform/security prerequisites. [C2–C4, R3–R6] |
| Optional Hermes client/invoker | Use authorized Coordinator MCP and/or Rex CLI if exact semantics/auth pass. Preserve Hermes capability evidence, but no host migration or second authoritative Kanban mirror is selected. [H1–H9] |
| Build only demonstrated gaps | Actual-session identity/publication bridge; scoped evidence/claim projection not already supplied; effect-owner integration only where native policy leaves a proven requirement unmet. |
| Conditional deletion candidates | Overlapping bespoke queue/profile/discovery/handoff/review/evidence code only where the chosen native replacement proves equivalent contracts and migrates state/callers. This is not an approved deletion list or edits. |
| Retain until replacement proves equivalence | No-replay/recovery/ownership fences, consent/security/isolation, PostgreSQL durable state and exact-source promotion policy. Existing code is not privileged merely because it exists. |
| Skip | New terminal multiplexer, mirrored authoritative brokers, speculative distributed lock framework, custom merge queue/CI scheduler/registry/GitOps controller. |

Hermes Kanban's single-host SQLite board is a real constraint of the native-fleet alternative, not a proposal to replace this project's PostgreSQL database or share SQLite over a network filesystem.
Remote peers/A2A do not by themselves demonstrate distributed Kanban worker ownership or safe adoption of external human terminals. [H3, H5–H7]
The user has selected Pi/OMP plus broker; host replacement is not an unresolved choice. Narrow actual-session integration must still prove identity, consent, admission/receipt, recovery and effect-authority contracts. A failed contract should be recorded precisely before adding infrastructure; do not use the choice to preserve every bespoke abstraction.

## 6. CI/CD conflict-to-native-control matrix

All controls below are recommendations or documented capabilities, **not assertions of current live configuration**.

| Conflict | Native control to reuse | Residual boundary |
| --- | --- | --- |
| Two agents edit related code | Separate worktrees/branches, PR diffs/reviews; shared pre-PR intent/dependencies | Worktrees isolate files, not semantics or external resources; coordinator facilitates overlap. [C6, G1] |
| Commits change after approval | Rulesets requiring appropriate reviews/CODEOWNERS, stale-review dismissal/latest-push approval, constrained bypass and protected refs | “Require PR” alone does not require approval; shared credentials erase actor distinctions. [G1] |
| Individually green PRs conflict on integration | Strict up-to-date checks; consider merge queue only by explicit policy choice and actual need | Queue requires `merge_group` validation; branch integration is not cross-repository release atomicity. No automatic merge approved. [G1, G2] |
| Old green run reused for new proposal | Latest applicable SHA/check source; independently bind immutable run, checkout source, proposal and attempt | Head, test-merge, merge-group and published-main SHAs differ; skipped/neutral required checks can count as passing. Aggregate gates must fail closed. [G3, G4] |
| Untrusted PR code reaches publishing secrets | Separate untrusted checks from trusted publication; minimal tokens, isolated runners, pinned actions | Privileged `pull_request_target`/`workflow_run` must not execute untrusted checkout; producer completion is not producer success. [G4, C1] |
| Repeated validation races | Actions concurrency keyed to replaceable PR/ref work | Repository-scoped, participating jobs only; default pending replacement and no dispatch-order guarantee. Not a global task queue. [G5] |
| Concurrent releases target one environment | One authorized path, shared participating Actions concurrency key where applicable, protected environment approval/refs | Environment is a gate, not a mutex; do not cancel stateful operations as rollback. Other repos/local tools/Argo are outside the group. [G5, G6] |
| Tag/source/producer mismatch | Build once, digest pins, verify expected signer/issuer/workflow and attested repo/source; review infra pins | Tags move; valid signature alone proves neither intended source nor safety. Image digest, chart checksum and OCI digest are distinct. [G7, K1, C1] |
| Agents edit live state against GitOps | Protected operator Git promotion; narrow Argo RBAC and single desired-state owner | Self-heal reverses imperative changes; unrelated Actions approval does not gate Argo auto-sync after infra merge. [A1, A4] |
| Two Applications own one object | One owner; `FailOnSharedResource=true` for tracked overlap | Does not detect distinct objects sharing DB/hostname/cloud effects or make a distributed transaction. [A2] |
| Maintenance races reconciliation | Owner-aware suspension, applicable sync windows and constrained override | ApplicationSet ownership matters; windows do not block arbitrary SQL/API/kubectl operations. [A1, A5] |
| Migration races another agent/release | One migration authority per DB, reviewed migrations/history and tool's actual DB-level serialization | PreSync/waves order one sync only; selective sync omits hooks; Jobs can execute twice; advisory locks bind only participants. [A3, K2, P1] |
| Synced/Healthy reported as accepted | Observe actual deployed pins/schema and exercise private TLS/auth/application control path | Resource health is not durability, authorization or no-replay proof; failed smoke does not undo effects. [A3, A6, C1] |
| Worker bypasses board/pipeline | Remove or constrain bypass-capable effect credentials and policy-edit authority; audit runner/workflow access | PATs, admin/override, kubeconfig, DB DDL or production secrets permit out-of-band writes; board ownership cannot prevent them. [G1, G6, A4] |

Hermes exact-head PR acceptance is useful evidence collection, but still needs this project's originating-run/checkout/provenance scrutiny.
A historical run's mutable PR association is not proof the current proposal was tested; retain immutable event-main and independently fetched proposal binding. [H8, G4, C1]
Actions concurrency does not serialize another repository, an agent's terminal, direct SQL or Argo.
Moving docs now describe additional queue options; their as-of-date/installed availability was not established and this recommendation does not depend on them. [G5]
Neither a board, a lease nor a status indicator promises exactly-once external effects.
A real safety boundary requires all consequential actors to use the authorized platform path, including humans and non-Hermes agents.

## 7. Discriminating evaluation plan — not executed

Run only after selecting permitted disposable fixtures and actual supported versions; none of the following has passed here.
Record observed identities, receipts, external state, denied actions and recovery outcome, not just screenshots or “done” labels.

| Scenario | Evidence that distinguishes a fit from a superficial demo |
| --- | --- |
| Native availability | Obtain exact installed Pi/OMP/Coordinator builds and, if required, supported Rex Linux artifact, license and headless/auth/storage instructions. Evaluate Hermes release/main compatibility only for optional MCP/Rex invocation actually chosen; Hermes installation is not a prerequisite to the selected fleet. |
| Independent human origin | Humans prompt two distinct profiles in already-open terminals. Coordinator sees each actual session's declared work/context without creating substitute SDK sessions or parent-spawned workers. |
| Profile/session isolation | Map terminal → profile → actual session → principal → task/attempt; distinguish managed worker session from human chat and prevent a second unmanaged profile writer. |
| Busy human plus handoff | Coordinator sends related work during a human turn; prove target session, queue/steer rules, consent, approvals, durable receipt and consumed/replied distinction without focus theft. |
| Relevant shared context | Publish API/schema dependency before PR creation; fresh updates reach coordinator despite frozen memory. Unrelated transcript/credentials remain private. |
| Cross-host without Desktop | Homelab coordinator and laptop specialist exchange work/result with Desktop closed; disconnect after admission and recover by exact run/delivery ID without duplicate execution. |
| Persistence boundaries | Separately disconnect clients, restart observer, restart owner/server and reboot host. Record process, transcript, event-gap and task recovery independently. |
| Uncertain external effect | Push a PR or begin a disposable migration, then lose owner/transport before receipt. Preserve unknown state, reconcile authoritative outcome and demonstrate no timeout-driven replay; inspect crash reclaim. |
| Exact run cancellation | Stop one admitted task while human/sibling work continues; verify target isolation, denied unauthorized stop, retained effects and operator reconciliation. |
| Stale CI/proposal | Approve A then push B; head changes mid-read, required job missing/skipped, old run reassociated. Reject stale evidence and verify actual checkout/producer and artifact identities. |
| Shared environment conflict | Competing worktrees/repos plus direct terminal attempt target one disposable DB/environment. Demonstrate which native fence blocks each actor; board warning alone fails. |
| GitOps acceptance | Approve unrelated source job then change infra pin; verify actual reviewed promotion boundary. Simulate Healthy with failing private route and retain failed/unknown acceptance. |
| Recovery/cutover | Export/restore board/task state plus required files, reconcile attempts/unknowns, verify ownership and rollback procedure without replay or dual authoritative brokers. |

The selected path is retained Pi/OMP plus narrow actual-session integration; these evaluations establish its implementation prerequisites, not another product-host selection exercise. Optional Hermes MCP/Rex invocation must pass its own authorization/identity tests without creating another board or taking fleet ownership.
Use Rex for terminal persistence only after its platform/access/security prerequisites pass; failure there does not justify a new orchestration engine or an automatic Hermes host migration.
Conditional deletions require explicit migration approval, every caller/state mapping, demonstrated replacement behavior and rollback/recovery evidence.

## 8. Unresolved prerequisites and decision boundaries

- Exact installed Coordinator artifact/source/host SDKs; Rex supported Linux/headless artifact and redistribution/license terms. Main/docs availability is not deployed stability. Evaluate stable-versus-main Hermes mapping only for optional client/invoker capabilities used.
- Actual Pi/OMP native profile selection and outer-session APIs; pinned public void callbacks lack durable per-input ID admission/cancel/drop/switch/restart/consumption receipts. Current private execution and real ModelProfile setup/smoke consumers remain until that safe successor and profile migration are proved.
- **Choice resolved:** actual Pi/OMP sessions + Coordinator broker; Hermes optional MCP/Rex CLI. Still specify designated coordinator identity, duplicate-coordinator/handoff policy and authenticated task/session binding.
- Publication mode: deliberate command/skill versus host event bridge; minimal relevant context, consent, freshness, privacy, receipt-versus-model-delivery and task/session binding.
- Hermes local profile-owner/dispatcher and SQLite-board topology remain historical constraints of the unselected native-fleet alternative, not prerequisites for this selected implementation. Optional remote client authentication and no-second-board behavior still require acceptance.
- Rex auth/remote authorization, server/host restart durability, event reconnect/replay, storage/backup and simultaneous human/machine input policy.
- Current GitHub plan/visibility, rulesets/check sources, review/bypass actors, workflow-edit authority and protected environment availability; no live audit performed.
- Actual Argo version, resource tracking, RBAC/default role, Application/ApplicationSet ownership, override and maintenance controls.
- Migration executor history/locking/connection scope, duplicate execution, transactional limits, schema compatibility, backup/restore and uncertain-effect reconciliation.
- Credential inventory across agents/humans/runners, actual producer/signer/source policy and all paths that can mutate the same environment/database.

The report establishes substantial native reuse candidates and specific discriminators, not turnkey equivalence or product impossibility.
Optional Hermes plugins, every A2A/auth path, complete release history and hostile-peer performance/security were not exhaustively audited.
All proposed evaluations remain unexecuted; existing historical fixture results are not evidence for these candidate integrations. [C1]

### Research-document verification

The parent research agent checked all 43 citation IDs, 57 Markdown links, local-link resolution and absence of temporary references/placeholders. Of 54 distinct external URLs checked, 49 returned HTTP 200; five pinned GitHub source-page URLs returned 503, while all five corresponding raw-source URLs returned 200. These are document/source-access checks, not runtime compatibility, installation, security or smoke acceptance; all scenarios above remain unexecuted.

## Sources

Primary sources only. Hermes links pin implementation where relevant; live docs, GitHub guidance and Argo `stable` pages can move independently of installed versions.
Rex API example `0.1.0` is not a verified release; its status protocol identifies revision 0.4 (2026-10-10). Kubernetes moving documentation contains later-version language; only longstanding digest/Job semantics are used.
Local numeric source ranges below deliberately preserve the original Coordinator snapshot, **not current imports or live infrastructure**. Moved owners are documented above and in the current consolidation map; old journal/runner-plugin/agent-contracts/evidence modules are not aliases or installable paths. External Hermes/Rex/GitHub/Argo evidence remains dated research, not rewritten as implementation proof.

- **C1 — Standing policy:** [deployment research](deployment-research.md), especially lines 19–44: exact-source gates, immutable trust, reviewed promotion, security, recovery and platform-claim boundaries.
- **C2 — Human versus SDK sessions:** `src/agent-session.ts:18-70,90-228`; `src/contracts.ts:269-291`; `src/plugin.ts:291-339,479-489`; `src/agent/contracts.ts:19-39`.
- **C3 — Broker/task/context:** `src/broker.ts:57-74,132-245,297-336,441-445`; `src/contracts.ts:41-52,90-168,362-426`; `src/plugin.ts:183-264,414-456`; `src/journal.ts:58-118`.
- **C4 — Ownership/control contracts:** `src/agent/contracts.ts:37-93,199-214,258-263`; `src/agent/client.ts:38-61`; `src/runner-plugin.ts:21-80`; `src/agent-session.ts:90-228`.
- **C5 — Evidence:** `src/evidence.ts:20-97`; `src/broker.ts:337-346,463-475`; `src/contracts.ts:384-388`.
- **C6 — Isolation/pipelines:** `src/runner.ts:267-298,496-524`; `.github/workflows/ci.yaml:1-11,153-162,260-286`; `.github/workflows/release.yaml:1-52,125-164`; `scripts/migrate.ts:8-25`; `README.md:74-78`.
- **R1 — Beta/platform/account commitments:** [Public testing is beginning](https://www.superlogical.com/updates/public-testing-beginning).
- **R2 — Actual acquisition:** [Installation](https://www.superlogical.com/rex/docs/installation), including HTML Discord download UI; [product plan](https://www.superlogical.com/).
- **R3 — Terminal identities/lifetime:** [CLI](https://www.superlogical.com/rex/docs/automate/cli).
- **R4 — Launch/capture:** [Shell scripting](https://www.superlogical.com/rex/docs/automate/shell-scripting); [Lua reference](https://www.superlogical.com/rex/docs/reference/lua).
- **R5 — API/scripts/remote configuration:** [Server API](https://www.superlogical.com/rex/docs/automate/server-api); [Lua scripts](https://www.superlogical.com/rex/docs/automate/lua-scripts); [Configuration](https://www.superlogical.com/rex/docs/customize/config).
- **R6 — Events and handler behavior:** [Lua events](https://www.superlogical.com/rex/docs/automate/lua-events); [Events reference](https://www.superlogical.com/rex/docs/reference/events).
- **R7 — Status trust/lifetime/roadmap:** [Program Status Protocol](https://www.superlogical.com/rex/docs/build/program-status).
- **H0 — Source/release identity:** [Pinned main commit](https://github.com/NousResearch/hermes-agent/commit/56808f8deeb8d2e9256a403fddff353f63eacd8d); [v0.22.2 release and wrong-commit correction](https://github.com/NousResearch/hermes-agent/releases/tag/v0.22.2); [annotated tag resolving stable commit](https://api.github.com/repos/NousResearch/hermes-agent/git/tags/7b77e29eedd85a8497ce2821448dac7964dc46f6).
- **H1 — Profiles:** [Guide](https://hermes-agent.nousresearch.com/docs/user-guide/profiles); [pinned profile implementation](https://github.com/NousResearch/hermes-agent/blob/56808f8deeb8d2e9256a403fddff353f63eacd8d/hermes_cli/profiles.py#L32-L95).
- **H2 — Independent/canonical chats:** [Bot Mode](https://hermes-agent.nousresearch.com/docs/user-guide/bot-mode).
- **H3 — Queue, runs, context and recovery:** [Kanban](https://hermes-agent.nousresearch.com/docs/user-guide/features/kanban).
- **H4 — Dispatch/notifications:** [Multi-gateway deployment](https://hermes-agent.nousresearch.com/docs/user-guide/features/kanban-multi-gateway).
- **H5 — Versioned worker routes/external lane limits:** [Worker lanes](https://hermes-agent.nousresearch.com/docs/user-guide/features/kanban-worker-lanes); [main dispatch argv](https://github.com/NousResearch/hermes-agent/blob/56808f8deeb8d2e9256a403fddff353f63eacd8d/hermes_cli/kanban_db_dispatch.py#L2651-L2706); [main receipt-waiting worker client](https://github.com/NousResearch/hermes-agent/blob/56808f8deeb8d2e9256a403fddff353f63eacd8d/hermes_cli/kanban_worker_client.py); [stable CLI-spawn route](https://github.com/NousResearch/hermes-agent/blob/d923a5eb487a17c0a68801bba9ef0f56c81040a8/hermes_cli/kanban_db_dispatch.py#L2644-L2700).
- **H6 — Messages/peer runs:** [Bot messaging and cross-machine peer routes](https://hermes-agent.nousresearch.com/docs/user-guide/bot-mode#bot-to-bot-messaging).
- **H7 — Cross-framework integration:** [A2A](https://hermes-agent.nousresearch.com/docs/user-guide/messaging/a2a).
- **H8 — Exact-head acceptance:** [PR contracts](https://hermes-agent.nousresearch.com/docs/user-guide/features/kanban#pr-completion-contracts); [main collector/classifier](https://github.com/NousResearch/hermes-agent/blob/56808f8deeb8d2e9256a403fddff353f63eacd8d/hermes_cli/kanban_pr_acceptance.py#L102-L191); [stable collector](https://github.com/NousResearch/hermes-agent/blob/d923a5eb487a17c0a68801bba9ef0f56c81040a8/hermes_cli/kanban_pr_acceptance.py#L102-L191).
- **H9 — External driving/MCP:** [Programmatic integration](https://hermes-agent.nousresearch.com/docs/developer-guide/programmatic-integration); [MCP client](https://hermes-agent.nousresearch.com/docs/user-guide/features/mcp); [curated MCP server tuple](https://github.com/NousResearch/hermes-agent/blob/56808f8deeb8d2e9256a403fddff353f63eacd8d/agent/transports/hermes_tools_mcp_server.py#L1-L88).
- **H10 — Children versus independent peers:** [Delegation](https://hermes-agent.nousresearch.com/docs/user-guide/features/delegation); [subagent lifecycle API](https://hermes-agent.nousresearch.com/docs/developer-guide/subagent-lifecycle-api).
- **H11 — Memory freshness/privacy scope:** [Memory](https://hermes-agent.nousresearch.com/docs/user-guide/features/memory).
- **H12 — Review/CI skills/triggers:** [GitHub skill](https://hermes-agent.nousresearch.com/docs/user-guide/skills/bundled/software-development/software-development-github); [SDLC review](https://hermes-agent.nousresearch.com/docs/user-guide/skills/bundled/devops/devops-sdlc-review); [webhooks](https://hermes-agent.nousresearch.com/docs/user-guide/messaging/webhooks).
- **H13 — Approvals/isolation/hooks:** [Security](https://hermes-agent.nousresearch.com/docs/user-guide/security); [hooks](https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks).
- **G1 — Branch/review/check/bypass rules:** [GitHub available rules](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets).
- **G2 — Integration serialization:** [GitHub merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue).
- **G3 — Latest/eligible check evidence:** [Required status troubleshooting](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks).
- **G4 — Trigger SHA/privileges:** [Events that trigger workflows](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows), especially `pull_request`, `pull_request_target` and `workflow_run`.
- **G5 — Concurrency:** [Actions concurrency controls](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).
- **G6 — Approval/refs/secrets/bypass:** [Deployments and environments](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments).
- **G7 — Producer/source evidence:** [Artifact attestations](https://docs.github.com/en/actions/concepts/security/artifact-attestations).
- **A1 — Desired-state reconciliation:** [Argo auto-sync](https://argo-cd.readthedocs.io/en/stable/user-guide/auto_sync/).
- **A2 — Tracked overlap:** [Fail on shared resource](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-options/#fail-the-sync-if-a-shared-resource-is-found).
- **A3 — Sync ordering/checks:** [Argo phases and waves](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-waves/).
- **A4 — Effect authorization:** [Argo RBAC](https://argo-cd.readthedocs.io/en/stable/operator-manual/rbac/).
- **A5 — Maintenance timing:** [Argo sync windows](https://argo-cd.readthedocs.io/en/stable/user-guide/sync_windows/).
- **A6 — Health scope:** [Argo resource health](https://argo-cd.readthedocs.io/en/stable/operator-manual/health/).
- **K1 — Artifact identity:** [Kubernetes image names/digests](https://kubernetes.io/docs/concepts/containers/images/#image-names).
- **K2 — Duplicate execution:** [Kubernetes Job failure handling](https://kubernetes.io/docs/concepts/workloads/controllers/job/#handling-pod-and-container-failures).
- **P1 — Database serialization limits:** [PostgreSQL 18 explicit/advisory locks](https://www.postgresql.org/docs/18/explicit-locking.html#ADVISORY-LOCKS).
