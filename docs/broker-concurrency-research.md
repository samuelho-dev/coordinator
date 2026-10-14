# Historical broker concurrency and durable SQLite transactions

**Superseded runtime:** this records the October12 SQLite investigation and worker correction, not the current beta.4 architecture. Broker and runner now require separate private external PostgreSQL18 databases with session-advisory ownership; the SQLite runtime adapter/dependency and worker files were deleted. Only the offline legacy importer uses `node:sqlite`. Historical measurements below remain evidence of scheduling versus physical I/O, not new PostgreSQL acceptance or proof that production contention is fixed. See [current operator procedures](self-hosting.md).

The authorized cutover is the existing live broker and homelab runner, not a separate beta or Mac runner. Mac plugin access remains through the broker; native SDK history is not Coordinator SQLite runtime. PostgreSQL ownership loss fences database serving/writes/readiness but preserves admitted native work until validated completion and cheap liveness200. Explicitly drain before restart; no automatic PostgreSQL reconnect or replay. Local kernel ownership loss remains a hard fence. Bounded current proof completed an actual native run under childUID3000000001 after PG backend death with ready503/live200 and retained the same prompt/run HELD without replay on restart; full suite/CI/publication/live acceptance are pending.

## Question and conclusion

Can Coordinator retain concurrent requests and Effect fibers while keeping correct, durable transactions, rather than treating storage isolation or reduced concurrency as the only remedy?

**Yes.** HTTP/fiber concurrency, independent SQLite readers, serialized SQLite writers, event-loop responsiveness, and physical-storage latency are different constraints. WAL supports concurrent readers and a writer on separate connections, but only **one writer per database**. Async or worker-backed execution does not increase that writer count. `synchronous=FULL` preserves WAL commit durability; moving database execution off the HTTP thread does not require weakening it. [1][2][3][4]

**Correction to the earlier recommendation:** storage isolation is one possible way to reduce the observed I/O contention, not a documented prerequisite for retaining concurrency and not yet the uniquely established fix. A database worker can address main-thread blocking; narrower transaction scopes and genuinely read-only connections can address unnecessary serialization; storage changes address slow physical I/O. These remedies solve different problems and may be complementary. The incident's precise blocking mechanism remains an **inference**, not a captured stack/syscall.

## Source-backed guarantees

- **Transactions and concurrency coexist.** SQLite automatically starts a transaction for a statement when needed. Separate connections can have simultaneous read transactions, each seeing its own stable snapshot, while only one write transaction is active. WAL allows reader/writer overlap; a long-lived reader can nevertheless prevent checkpoint progress. One shared connection does not provide the separate-reader concurrency described here. [1][2]
- **Read-only work should not always use `BEGIN IMMEDIATE`.** It immediately starts a write transaction, even before any write statement, and can wait/fail behind another writer. A single SELECT already has an implicit transaction; a multi-query consistent read can use `BEGIN`/`BEGIN DEFERRED`. Read-then-write atomic operations may appropriately reserve the writer with `BEGIN IMMEDIATE` to avoid upgrade failures. Do not reclassify heartbeat/poll merely from their names: first inspect every database mutation and required snapshot/atomicity boundary. [2]
- **Durability has an I/O cost, not a requirement to serialize all HTTP work.** WAL + FULL performs an additional WAL sync for each commit; NORMAL can lose acknowledged commits after power loss/system crash, despite preserving consistency in WAL. NORMAL/OFF is therefore not an equivalent remedy when power-loss durability must be retained. SQLite's guarantees assume the underlying storage implements the required behavior correctly. [1][3][10]
- **Busy timeout is lock-wait policy, not an operation deadline.** The busy handler sleeps until its configured accumulated sleep time and then returns `SQLITE_BUSY`; SQLite may bypass it to avoid deadlock. It does not cap slow reads, writes, checkpoints, or storage sync latency. Reducing it can shorten synchronous lock waits but can also turn contention into earlier errors; it does not fix stalled storage. [5][6]
- **Checkpoint latency is distinct from commit sync latency.** SQLite's default automatic checkpoint threshold is 1,000 WAL pages; the committing thread runs the checkpoint by default, making some commits slower. Checkpoints can instead be scheduled in another thread/process. That does not remove FULL's per-commit WAL sync, and long reader snapshots can retain WAL pages. [1]

## Historical inspected adapter and runtime facts

At the **2026-10-12 investigation**, dependencies pinned `@effect/sql-sqlite-node` and `@effect/platform-node` to **4.0.2**, with `effect` at commit **d6082a25a3779f7e2abba2787323d5d9c7e1d24a**, and Node >=22.16. The **2026-10-13 historical follow-up** used published Effect/platform-node/sql-sqlite-node4.0.3. The current PostgreSQL cutover removes sql-sqlite-node entirely. Preserve the versioned citations below; current package/source files do not reproduce that historical adapter.

The official 4.0.2 SQLite adapter source confirms: [7]

- It constructs `node:sqlite.DatabaseSync`; prepares and executes statements through synchronous `Effect.try` callbacks calling `db.prepare`, `statement.all`, and `statement.run`.
- It creates **one connection** and a semaphore with **one permit**. The pinned Effect SQL implementation acquires that permit for the transaction scope and releases it on scope finalization. Waiting fibers and executing native SQLite are different: permit wait does not itself turn a native synchronous call into async work. [7][8]
- Writable clients use `BEGIN IMMEDIATE`; `readonly: true` clients open a read-only connection and use plain `BEGIN`.
- WAL is enabled by default on writable clients; `busyTimeout` defaults to five seconds and is applied with `PRAGMA busy_timeout`.
- There is **no async/worker mode option for query execution**. `backup()` uses Node's Promise-returning backup API, an exception that does not make normal queries asynchronous. [7]

The inspected historical broker explicitly set WAL, FULL and a5,000ms busy timeout and wrapped non-remote commands in `sql.withTransaction`. This is not current broker wiring. The prior investigation recorded matching local/deployed adapter SHA-256 `a3c53f5ab983ed5338c3f829cc9d8f11e54f55182f119720f83c1aea3a0c7820`; inherited incident evidence, not fresh production verification.

Node24 documents all `DatabaseSync` APIs as synchronous. The pinned Effect runtime evaluates synchronous thunks directly; Effect fibers, `runPromise`, or a Promise wrapper do not automatically offload those thunks. **Inference from these implementations:** while a SQLite native call waits on a lock or storage on the HTTP thread, that thread cannot service unrelated HTTP/timer work until the call returns. [4][7][9]

## Alternatives preserving concurrency and durability

These are candidate designs, not implemented changes or proven incident fixes.

| Candidate | What it addresses | Boundary/tradeoff |
| --- | --- | --- |
| Keep concurrent handlers/fibers; reduce unnecessary transaction scope and use the installed adapter's read-only client for genuinely read-only snapshot work | Avoids reserving the sole writer for pure reads; separate connections enable WAL reader/writer overlap [1][2][7] | Same-thread synchronous calls still block that thread. Additional connections require correct routing; long snapshots can impede checkpoints. Keep actual write invariants atomic. |
| Own SQLite connection/transactions in a long-lived native Node worker; use existing Effect worker transport if suitable | Separates synchronous database execution from the HTTP event loop without a new SQLite package [4][11][12] | **Inference:** HTTP can remain responsive during that worker's native wait, but DB-dependent requests still wait. Keep each transaction's connection and sequence owned by the worker; do not interleave unrelated requests inside an open transaction. Messaging, shutdown, failure and acknowledgment semantics still require deliberate integration. |
| Move checkpoint work off the request execution thread while retaining FULL | Avoids automatic checkpoint work on selected request commits [1][3] | Does not eliminate FULL commit sync, physical I/O contention, or checkpoint starvation. Change only if checkpoint work is the measured bottleneck. |
| Reduce competing storage writes or change physical storage placement | Targets the incident's observed I/O contention | **Hypothesis:** improves commit/query tail latency if device contention is causal. Does not change SQLite's writer limit or inherently remove same-thread synchronous calls. Physical placement and scheduler evidence must distinguish real isolation from merely a different volume path. |

Installed/native first: Node provides `worker_threads`, and installed `@effect/platform-node` 4.0.2 supplies `NodeWorker`/`NodeWorkerRunner` transport. Neither is an automatic worker-backed SQLite client. Node's general advice favors built-in asynchronous I/O over workers for I/O workloads; here the relevant limitation is that the SQLite query API being used is synchronous. A new async SQLite dependency or database migration is **not established as necessary** by the documentation. [4][7][11][12]

## Kubernetes and Linux I/O isolation

- **CPU/memory reservations are not disk reservations.** Ordinary Pod CPU/memory requests and limits do not reserve disk IOPS, bandwidth, or latency. `ephemeral-storage` requests/limits account for capacity in bytes; Kubernetes explicitly provides no local ephemeral-storage IOPS performance SLA. Neither setting establishes performance isolation for this broker's local-path PVC. [13][14]
- **CSI performance controls are driver-specific.** `VolumeAttributesClass` can carry parameters such as IOPS only when the CSI driver implements the corresponding volume-modification capability. It is not a generic Pod IOPS reservation or an automatic capability of local-path storage. [15]
- **Kernel I/O controls are conditional mechanisms, not guarantees.** cgroup v2 `io.max` imposes per-device BPS/IOPS ceilings, not minimum reservations; `io.weight` and `io.latency` provide conditional relative/latency protection. Their applicability depends on the enabled controller, hierarchy, device and filesystem. Buffered-write attribution requires filesystem writeback support, and block-I/O limits do not immediately cap initial page-cache dirtying. No supported Kubernetes/Talos configuration for applying these controls to this deployment was established in this research. [16]
- **Placement and data migration are different.** Kubernetes local PV affinity constrains the consuming Pod to the node holding its data. Local-path can provision local or hostPath volumes; the actual broker PVs were previously observed with worker-2 node affinity. Changing affinity does not copy their contents. Moving CI to an independent node/device can preserve CI concurrency without moving broker data; moving the broker requires making its durable data available at the destination. A different PVC directory on the same device is not physical I/O isolation. [17][18]
- **Throughput alone does not establish saturation.** Device IOPS, queueing, request/flush latency and PSI stall time are separate measurements. The recorded I/O pressure and write-latency spike support contention as a hypothesis, but neither the aggregate CI bandwidth nor low broker CPU proves the blocked SQLite operation or makes a volume migration necessary. [19][20]

## Local mechanism verification

An isolated check used the deployed runtime versions, Node24.21.0 and SQLite3.53.4, with the exact installed adapter. An independent process held a SQLite writer lock for approximately 1.8 seconds; another thread requested unrelated HTTP health. Transaction acquisition, statement execution, commit and checkpoint were measured separately.

| Main-thread operation | Observed time |
| --- | ---: |
| Baseline HTTP health | 29 ms |
| `BEGIN IMMEDIATE` writer-lock wait | 1,747 ms |
| HTTP health during that wait | 1,765 ms |
| Insert | 0.47 ms |
| `COMMIT`, `synchronous=FULL` | 1.21 ms |
| FULL checkpoint, all 3 WAL pages | 1.06 ms |

This verifies synchronous lock-wait starvation on the deployed runtime versions, not the exact production disk-contention cause. In this isolated run, commit and checkpoint were fast; the writer-lock wait blocked unrelated HTTP. Temporary processes and database files were removed.

## Evidence needed before selecting an implementation

The previously observed I/O-stall/disk-write-latency spike and co-located CI write load correlate with the outage. They do not identify whether the broker was inside a busy-lock wait, `fsync`/`fdatasync`, a read/write, checkpoint, backup, or another synchronous operation. Low broker CPU does not select among those waits.

Next discriminating evidence should be gathered locally or during an explicitly authorized diagnostic window:

1. Align HTTP latency/event-loop delay with database permit queue wait, transaction duration, statement execution and commit duration. Distinguish healthy HTTP with slow DB-dependent responses from a blocked HTTP thread.
2. Capture the native thread stack/syscall during a stall, plus lock-owner/other-connection activity and WAL/checkpoint state. Separately measure backup/checkpoint execution; do not infer `fsync` solely from FULL configuration.
3. Establish actual deployed Node patch/bundled SQLite version, source revision, connection owners, and transaction write sets. Determine which operations need a write lock, which need only a consistent read snapshot, and which can be one implicit statement.
4. Compare the same workload with concurrent handlers retained: baseline, database execution in a worker, and competing storage traffic controlled. Retain FULL in these comparisons. A worker improving HTTP but not commit latency identifies scheduling isolation; lower commit latency after reducing competing I/O identifies a storage contribution. Both can be true.

**Research-phase decision:** retain concurrency and FULL durability; distinguish event-loop blocking from physical-storage latency before choosing worker execution, narrower read/write transaction routing, checkpoint changes, or workload/storage isolation. Storage migration and reduced CI concurrency are not prerequisites.

## Historical implemented scheduling isolation (deleted runtime)

The now-deleted `src/sqlite.ts` and `src/sqlite-worker.ts` used the installed Effect worker/RPC infrastructure and native SQLite adapter. Each database had one worker owner. Broker ledger/snapshot sanitization executed outside HTTP; the then-current runner stayed SQLite. Scoped permits covered asynchronous statement completion, transactions and backup; savepoints, COMMIT cleanup, interruption rollback and FULL durability remained intact. PostgreSQL ownership now supersedes this implementation.

Worker loss permanently fences the connection and revokes broker authorization/serving; there is no worker replacement or work replay. RPC uses `Schema.toCodecIso` on both ends: it reconstructs exits/errors while preserving native bigint/blob values through structured clone. The default worker JSON codec rejects bigint inside `Unknown`; passing schemas through unchanged loses `Exit` reconstruction.

An isolated native diagnostic delayed the real ledger WAL `fsync` by 1,600 ms before calling the original syscall. Both clients retained `PRAGMA synchronous=2` (FULL), and the acknowledged row was read back.

| Execution location | FULL commit | Unrelated HTTP health |
| --- | ---: | ---: |
| HTTP thread, original adapter | 1,603 ms | 1,519 ms |
| SQLite worker | 1,603 ms | 57 ms |

The worker isolates scheduling, not storage latency: DB-dependent requests still wait for disk/locks. The delay was synthetic; this is not a production syscall capture or a claim that disk latency improved.

The actual `src/main.ts broker` CLI also ran under Node24.21.0: authenticated health returned in28 ms while an MCP heartbeat waited1,869 ms for an external writer. The session lease renewed after release, the same bound session polled successfully, and SIGTERM closed the process. The permanent real-broker regression failed on the original synchronous wiring with a500 ms health timeout and passed after cutover. Separate native checks exercised bigint/blob/SafeIntegers, transaction visibility, savepoint rollback, interrupted transaction cleanup, failed COMMIT/inactive-transaction recovery, backup reopening and fail-closed owner termination. Terminating the actual ledger worker revoked HTTP serving rather than leaving health green.


Historical development-worktree verification: native build and pinned-Node24 typecheck passed; the complete serial suite passed96 tests under UID1000/six capabilities/NoNewPrivs/private HOME. Its isolated deployment correction passed typecheck and real HTTP contention separately. These are worker-era results, not beta.4 gates; current PostgreSQL tests supersede that regression wiring. Diagnostic state was removed. No storage migration, CI concurrency reduction or replay was performed in that investigation.

## Primary sources

1. [SQLite WAL: concurrency, checkpointing, performance](https://sqlite.org/wal.html#concurrency).
2. [SQLite transactions: read/write transactions and DEFERRED/IMMEDIATE](https://sqlite.org/lang_transaction.html).
3. [SQLite `PRAGMA synchronous`: FULL/NORMAL durability](https://sqlite.org/pragma.html#pragma_synchronous).
4. [Node24 `DatabaseSync`: synchronous APIs](https://nodejs.org/docs/latest-v24.x/api/sqlite.html#class-databasesync).
5. [SQLite busy timeout](https://sqlite.org/c3ref/busy_timeout.html).
6. [SQLite busy handler: deadlock exceptions](https://sqlite.org/c3ref/busy_handler.html).
7. [Effect 4.0.2 Node SQLite adapter source](https://github.com/Effect-TS/effect/blob/effect%404.0.2/packages/sql/sqlite-node/src/SqliteClient.ts).
8. [Pinned Effect SQL client: `makeSqliteAcquirers`, transaction ownership](https://github.com/Effect-TS/effect/blob/d6082a25a3779f7e2abba2787323d5d9c7e1d24a/packages/effect/src/sql/SqlClient.ts#L298-L349).
9. [Pinned Effect runtime: synchronous thunk evaluation](https://github.com/Effect-TS/effect/blob/d6082a25a3779f7e2abba2787323d5d9c7e1d24a/packages/effect/src/internal/effect.ts#L965-L971).
10. [SQLite atomic commit: hardware and operating-system assumptions](https://sqlite.org/atomiccommit.html).
11. [Node24 worker threads: parallel JavaScript and I/O guidance](https://nodejs.org/docs/latest-v24.x/api/worker_threads.html).
12. [Effect 4.0.2 `NodeWorker`: installed worker/child-process transport](https://github.com/Effect-TS/effect/blob/effect%404.0.2/packages/platform/node/src/NodeWorker.ts).
13. [Kubernetes resource management: CPU, memory and storage resources](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/).
14. [Kubernetes local ephemeral storage: capacity and lack of IOPS SLA](https://kubernetes.io/docs/concepts/storage/ephemeral-storage/).
15. [Kubernetes VolumeAttributesClass: CSI-specific parameters](https://kubernetes.io/docs/concepts/storage/volume-attributes-classes/).
16. [Linux cgroup v2: I/O controller, latency protection and writeback](https://docs.kernel.org/admin-guide/cgroup-v2.html#io).
17. [Kubernetes local volumes: required node affinity](https://kubernetes.io/docs/concepts/storage/volumes/#local).
18. [Local-path provisioner: volume types and node affinity](https://github.com/rancher/local-path-provisioner#node-affinity-key).
19. [Linux block I/O statistics](https://docs.kernel.org/admin-guide/iostats.html).
20. [Linux Pressure Stall Information](https://docs.kernel.org/accounting/psi.html).
