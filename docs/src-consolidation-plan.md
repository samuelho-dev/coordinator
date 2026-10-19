# Implemented native-session inspector: source map and metric catalog

This is the **implemented inspector cutover**, not a consolidation proposal. The old 18-file execution layout and 75/69 command catalogs are retired. Current model-facing tools are only `session.list` and `session.inspect`. Product flow, exact package commands and observed local acceptance are in the [README](../README.md); operator procedures are in [self-hosting](self-hosting.md). No installation, deployment, native profile overwrite, live-service stop, historical import, state deletion or commit was performed by this cutover.

## Complete current source tree

```text
src/
├── inspection.ts   # boundary schemas, metric/provenance contracts and two tool descriptors
├── resources.ts    # plain Linux/Darwin process identity and on-demand sampling functions
├── inspector.ts    # Inspector service: authenticated ephemeral registry/read RPC owner
├── mcp.ts          # Mcp service: bounded HTTP/MCP client/server and stdio transport
├── native.ts       # plain native projections, private metadata cache and outbound connectors
├── plugin.ts       # actual-session Pi/OMP observer and native extension entrypoint
└── main.ts         # CLI/root composition; no execution role
scripts/
├── install.ts      # explicit native-host installer
├── smoke.ts        # disposable real native CLI/HTTP/connector smoke
└── deployment-smoke.ts # exact-image authenticated inspector smoke
```

The seven `src/` files are handwritten; there is no generated runtime subtree. Removed execution modules include broker, runner, agent-session, platform, contracts, database, backup, the three agent modules and five generated runtime artifacts, along with old execution tests/tooling. The Hermes Python integration, decoder/projector and plugin manifest are removed; native command hooks use the existing TypeScript CLI, with no Coordinator-added Python dependency. Upstream Hermes itself still uses its native Python runtime. `prisma/` and `migrations/` are **retained historical rollback source assets**, not current runtime dependencies. Real databases, journals, backups, claims/PVCs and old images remain preserved; removal of source is not permission to delete durable state or start old/new owners together.

## Actual owners, layers and callable surfaces

There are **two application `Context.Service` owners**, not one service per harness or command. Plain functions below do not acquire a second application service merely by being imported.

| File / owner | Current surface and responsibility |
| --- | --- |
| [`inspection.ts`](../src/inspection.ts) | `SessionRef`, `SessionPresence`, metric/observation/config/connector schemas; strict `decode*` boundary functions; `sessionRefKey`, `metric`, `unavailableMetric`, `createInspection`; `InspectionTools.list` / `.inspect` descriptors. `HttpError` uses installed `Schema.TaggedError`. No layer/state owner. |
| [`inspector.ts`](../src/inspector.ts) / `Inspector` | `make(config)` acquires scoped ephemeral registry state; `layer(config)` is `Layer.effect(Inspector, make(config))`. Methods: `authorize(token)`, `list(principal,input)`, `inspect(principal,input)`, `announce(principal,input)`, `poll(principal,input)`, `complete(principal,input)`, `disconnect(principal,input)`. All return typed Effects with `HttpError`; receiver clock owns liveness. No database, native process execution or server dependency. |
| [`mcp.ts`](../src/mcp.ts) / `Mcp` | `make`, `layer = Layer.effect(this,this.make)`. Methods: generic `call(url,token,descriptor,params,timeoutMs?)`, typed JSON `post(url,token,path,inputSchema,outputSchema,params,timeoutMs?)`, scoped `serve(server,inspector)`, generic `serveStdio(reader)` where reader has `list(input)` / `inspect(input)` Effects. Descriptor/reader inference preserves success/error/requirements. `serverLayer(host,port)` supplies the Node HTTP server. |
| [`resources.ts`](../src/resources.ts) | `identifyProcess(pid)` reads birth/parent identity only; `observeResources(roots)` samples attributable process trees on demand; `parseDarwinSnapshot(output)` parses bounded native `ps` counters. `ResourceError` and plain resource types, no Context service/layer. |
| [`native.ts`](../src/native.ts) | `readPrivateFile`, `loadConnectorConfig(file?)`, `readConnectorToken`; scoped `connectNative(config,{presence,inspect})`; `withProcessResources(inspection)`; boundary projectors `projectHook`, `projectClaudeStatus`, `projectCodexNotification`; `writeHermesHook(config,envelope,at=Date.now(),hostPid=process.ppid)` validates native Hermes events and actual dispatcher-host identity; private-cache `writeNativeCache` / `readNativeCache`; `claudeRoster(config)` and `connectExternal(config)`. `NativeError`; no native SDK/session owner. |
| [`plugin.ts`](../src/plugin.ts) | `createNativeObserver(harness,machineId,processes?)` returns plain `update`, `presence`, `inspect`, `clear`; retained state is current native context, scalar latest usage and active tool identifiers, not event/message history. `isCoordinateInput`, `transformCoordinateInput`, `registerCoordinateKeyword`; default extension registers native lifecycle/input hooks, creates Node/Mcp ManagedRuntime and scoped outbound connector. No Plugin Context service. |
| [`main.ts`](../src/main.ts) | `cli(args)` scopes modes. `inspector` decodes private config, calls `Inspector.make`, builds `serverLayer`, passes actual server/inspector to `Mcp.serve`. `connector` calls `connectExternal`; `mcp` binds remote `Mcp.call` reader to `serveStdio`; `list`/`inspect` invoke those descriptors; native stdin observers project and cache bounded metadata, with `hook hermes` calling `writeHermesHook`. Executable root provides `Layer.mergeAll(NodeServices.layer,Mcp.layer)` through `NodeRuntime.runMain`. Imports are inert. |

`Inspector.inspect` forwards one bounded transient read request to the current connector incarnation; timeout/cancellation removes it. `announce`/`poll`/`complete`/`disconnect` map to Bearer-authenticated `/connector/announce`, `/connector/poll`, `/connector/result`, `/connector/disconnect`, **not model tools**. `authorize` binds owner + machine; reads are same-owner, writes only the principal's machine. Registry restart loses discovery until reannouncement, never stops native agents. Presence repeats every 10 s; receiver liveness is 30 s, expiry 120 s, query deadline 10 s; detail callback deadline 8 s. Reconnect concerns read-only presence/RPC, not task replay.

`Mcp.serve` exposes `/mcp` with both tools and readOnlyHint, minimal public `/livez`, authenticated `/readyz` and `/healthz`. Installed Effect MCP protocol adapters include 2026-07-28 and supported older versions; this is not the retired two-owner execution protocol. TLS except loopback, no redirects/token forwarding, Origin protection, 64 KiB HTTP/native input boundaries and bounded MCP structured/framing/SSE output remain enforced. Detail is at most **48 KiB**; native resource projection reserves 2 KiB normalization headroom. Truncated process details explicitly mark partial while retaining the actual aggregate scope, rather than recomputing aggregates from displayed rows.

## Identity, state and provenance

`SessionRef` is `machineId + harness + nativeSessionId`, with optional actual `nativeAgentId` to distinguish actors where supplied. Presence supports native parent/label/kind, provider/model/profile/agent, cwd/repository, Rex server/block/session/window/pane/terminal IDs, OS/machine/version metadata, process PID/birth/ownership, activity/wait/approval/streaming/compaction/current tool and bounded native goals/todos **only when supplied**. Schema support does not mean all adapters collect those fields. Pi/OMP expose current model/cwd/activity, OMP actual agent identity, and available Rex session/block environment metadata; hooks/roster/Hermes project only their whitelisted fields. No prompt, conversation, queued text, system prompt or tool input/result is transmitted.

Every numeric value is either `available` with value/unit or `unavailable` with null/reason. Provenance carries `source`, `scope`, `scopeDescription`, `observedAt`, `nativeUpdatedAt` (nullable), `estimated`, `derived`, `shared`, `partial`, `stale`, and optional `sampleWindow`. `queriedAt` is not the native update time. Cached counters retain original observation/native timestamps; unknown update times stay null. Connection receipt/liveness is receiver-owned and separate from counter freshness/native activity. In particular, a live connector is not proof of a live metadata-only Codex process.

PID + birth protects attribution/reuse: Linux boot UUID + start ticks; Darwin C-locale UTC `lstart` has **one-second resolution**, so same-second PID reuse cannot be distinguished. Registry resolves competing process claims without double owned charging. Shared/unknown processes can have per-process measurements but no fabricated owned aggregate; OMP and Hermes host PIDs are shared. CPU 100% = one core. RSS sums are not unique physical RAM. OS-derived values retain actual observation times/windows with `derived:true`, `nativeUpdatedAt:null`; unsupported/inaccessible/exited/reused processes are not zeroes.

### Hermes native command-hook boundary

`node dist/src/main.js hook hermes` consumes bounded native JSON for exactly `on_session_start`, `on_session_end`, `on_session_finalize` and `post_api_request`. Only start establishes a record; end means idle/turn end, not session closure. Finalize closes `extra.old_session_id` when supplied, otherwise `session_id`; late usage/end/finalize callbacks cannot recreate a closed or absent record. Profile is top-level `profile`; model, provider, usage and old session identity come from `extra`. Successful `post_api_request` reads `extra.usage` into the six existing latest-request token counters, never lifetime/account usage or cost. Input/output aliases use the first non-null supplied value (`input_tokens` before `prompt_tokens`, `output_tokens` before `completion_tokens`); zero remains available and absent/null remains unavailable. A later request with missing usage does not reuse older request counters; lifecycle metadata updates preserve the latest request counters and their original freshness.

The installer emits `hooks.yaml` (JSON-compatible YAML), `mcp.yaml`, `mcp.json`, `connector.sh` and `mcp.sh` into a new private directory, never the selected native configuration. Manually append only generated observer hooks and merge only `mcp_servers.coordinator` into the chosen Hermes profile's `config.yaml`, preserving existing hooks/MCP settings and approving the intended commands through native consent. No plugin installation/enable step is needed; MCP-reader-only Hermes may omit observation hooks. Generated hook commands are direct argv execution: `/usr/bin/env COORDINATOR_CONNECTOR_CONFIG='<config>' '<node>' '<main>' hook hermes`. The supported native dispatcher uses `subprocess.Popen(..., shell=False)`; env replaces itself with Node, so Node's actual parent is the dispatcher host, not an intermediary shell. Its OS PID/birth is validated and bound as **shared**, never an exclusive session process, a wire-supplied PID or a guessed TUI ancestor.

The canonical private Hermes cache is `<stateDirectory>/native-observations/hermes-<sha256(sessionRefKey(ref))>.json`, using the standard inspection record, owner-only directory/files and bounded cache/input sizes. Unknown/legacy files are retained, not migrated or deleted. While the exact bound host remains alive, cache reads refresh presence `observedAt` only; they do not freshen native activity, usage timestamps or native update times. Idle sessions therefore remain discoverable without a heartbeat daemon or periodic CPU/RSS/detail sampling; resource sampling remains on demand. Without an available host identity, no process/resource attribution is supplied and metadata expires after 120 seconds. A dead/reused bound host is absent from reads immediately, with expired records removed on cache access. The Darwin one-second identity limit and unavailable owned aggregates for shared hosts still apply.

## Complete numeric metric catalog

This is the single complete numeric allowlist reference, matching [`inspection.ts`](../src/inspection.ts); the README links here rather than duplicating it. **All availability is conditional on a real native field/sample being present.** Metrics initialized by `createInspection` remain explicitly unavailable unless one of the listed projections supplies them. No transcript reconstruction, full SessionStats/history scan, event-accumulator lifetime estimates, independent ratecard pricing or parent/child sum is used.

Source abbreviations used below:

- **OS**: Linux `/proc/<pid>/stat`, boot identity and native `getconf` units; Darwin `/bin/ps -axo pid=,ppid=,lstart=,time=,rss=`. Two snapshots approximately 200 ms apart, validated ancestry/birth; no command lines or host totals. See `resources.ts` and `native.withProcessResources`.
- **P/O context**: Pi/OMP public `ctx.getContextUsage()` (`contextWindow`, nullable `tokens`, nullable `percent`), `current-context`, estimated; unknown after compaction stays unknown.
- **P/O latest**: whitelisted latest native assistant `usage`, `latest-native-update`, original native timestamp if supplied. Cost values are native client estimates, not a new price engine.
- **O aggregate**: OMP `ctx.sessionManager.getUsageStatistics()`, `native-aggregate`, finalized cached indexed session entries, **not selected branch/lifetime by assumption**; total cost includes completed direct-child spend, never add child costs again. No aggregate reasoning/component-cost/credit counters.
- **O async**: OMP `ctx.getAsyncJobSnapshot()`, current-session native snapshot; no commands/labels read.
- **H latest**: Hermes native command-hook `post_api_request` successful request `extra.usage`, `latest-native-update`, not a session aggregate. `input_tokens`/`prompt_tokens` and `output_tokens`/`completion_tokens` are alternative native fields; unknown native update time stays null.
- **C status**: optionally manually composed Claude statusline payload; latest token/context figures are **current-context/latest-response**, not lifetime totals. Native client session cost is estimated; native duration semantics retained.
- **X attached**: Codex **v2** `thread/tokenUsage/updated` from an already attached app-server, `last` → latest update and `total` → native attached-thread aggregate. Ordinary hooks are metadata only; no observer of arbitrary other TUIs, no hidden app-server, no fabricated PID or thread USD cost.

### Process resources

| Exact metric | Unit / scope | Actual source and unavailable limits |
| --- | --- | --- |
| `cpuPercent` | percent-one-core / process sample | OS CPU-time delta divided by actual monotonic sample interval; 100% = one core. Unavailable without stable readable identified process/sample. |
| `cpuTimeMs` | milliseconds / process | OS cumulative process CPU time (not session running time); derived native unit conversion. Same identity/readability limits. |
| `rssBytes` | bytes / process | OS resident pages × page size (Linux) or native RSS KiB × 1024 (Darwin); not unique RAM. |
| `ownedProcessCount` | count / owned-processes | Unique exclusively attributable roots/descendants; unavailable on partial/truncated/shared/unknown attribution or absent roots, not invented zero. |
| `ownedCpuPercent` | percent-one-core / owned-processes | OS unique exclusively attributable CPU sum, may exceed 100%; shared/unknown attribution makes aggregate unavailable; partial flags preserved. |
| `ownedRssBytes` | bytes / owned-processes | OS unique exclusively attributable RSS sum, **not** unique physical RAM; shared/unknown attribution makes aggregate unavailable; partial flags preserved. |

### Context

| Exact metric | Unit / scope | Actual native source(s) and unsupported status |
| --- | --- | --- |
| `contextWindowTokens` | tokens / current-context | P/O context `contextWindow`; C status `context_window.context_window_size`; X attached `modelContextWindow` when non-null. Hermes unavailable. |
| `contextUsedTokens` | tokens / current-context | P/O context `tokens` when known; C status derived only with all current input + cache-read + cache-creation operands, **excluding output**. X attached/Hermes unavailable (no compatible input-only used basis). |
| `contextRemainingTokens` | tokens / current-context | Derived `max(0,window-used)` only for compatible known P/O or C status basis. X attached/Hermes unavailable. |
| `contextUsedPercent` | percent / current-context | P/O context `percent`; C status native `used_percentage`. X attached/Hermes unavailable. Preserve over-limit values. |
| `contextRemainingPercent` | percent / current-context | P/O derived `100-percent` with valid positive window; C status native `remaining_percentage`. X attached/Hermes unavailable. |
| `systemPromptTokens` | tokens | Unavailable in all implemented adapters: public context getters do not expose this native breakdown. Never read/tokenize system prompt text. |
| `systemToolsTokens` | tokens | Unavailable in all implemented adapters: no public native breakdown counter. |
| `systemContextTokens` | tokens | Unavailable in all implemented adapters: no public native breakdown counter. |
| `skillsTokens` | tokens | Unavailable in all implemented adapters: no public native breakdown counter. |
| `messagesTokens` | tokens | Unavailable in all implemented adapters: no public native breakdown counter; no history scan. |

### Tokens

Each exact name occurs in **both** `tokens.latestNativeUpdate` and `tokens.nativeAggregate`; a populated latest value is not copied into aggregate. Cache can be a subset of input and reasoning a subset of output: never blindly sum fields to invent total.

| Exact metric | Unit | Latest native update sources | Native aggregate sources / limits |
| --- | --- | --- | --- |
| `inputTokens` | tokens | P/O latest `input`; H latest `input_tokens` or `prompt_tokens`; C status `context_window.total_input_tokens` (current-context/latest semantics); X attached `last.inputTokens`. | O aggregate `input`; X attached `total.inputTokens`. Pi/Hermes/Claude aggregate unavailable. |
| `outputTokens` | tokens | P/O latest `output`; H latest `output_tokens` or `completion_tokens`; C status `context_window.total_output_tokens` (not lifetime); X attached `last.outputTokens`. | O aggregate `output`; X attached `total.outputTokens`. Others unavailable. |
| `cacheReadTokens` | tokens | P/O latest `cacheRead`; H latest `cache_read_tokens`; C status `context_window.current_usage.cache_read_input_tokens`; X attached `last.cachedInputTokens`. | O aggregate `cacheRead`; X attached `total.cachedInputTokens`. Others unavailable. |
| `cacheWriteTokens` | tokens | P/O latest `cacheWrite`; H latest `cache_write_tokens`; C status `context_window.current_usage.cache_creation_input_tokens`; optional X attached `last.cacheWriteInputTokens`. | O aggregate `cacheWrite`; optional X attached `total.cacheWriteInputTokens`. Missing optional native counter is unavailable. |
| `reasoningOutputTokens` | tokens | Pi latest `reasoning`; OMP latest `reasoningTokens`; H latest `reasoning_tokens`; X attached `last.reasoningOutputTokens`, only when actually supplied. Claude unavailable. | X attached `total.reasoningOutputTokens`; O aggregate has no reasoning counter; other aggregates unavailable. |
| `totalTokens` | tokens | P/O latest `totalTokens`; H latest `total_tokens`; X attached `last.totalTokens`. Claude unavailable, no inferred sum. | O aggregate `totalTokens`; X attached `total.totalTokens`. Others unavailable. |

### Cost/provider usage

Each name occurs in **both** `cost.latestNativeUpdate` and `cost.nativeAggregate`. USD values below are native client estimates where flagged, not invoiced spend. No model cost ratecard is treated as an actual usage counter; Hermes account usage is not session usage.

| Exact metric | Unit | Latest native update sources | Native aggregate sources / limits |
| --- | --- | --- | --- |
| `totalCostUsd` | usd | P/O latest `usage.cost.total`, estimated. Other latest scopes unavailable. | O aggregate `cost`, estimated, including completed direct-child spend; C status `cost.total_cost_usd`, client-estimated session cost. Pi/Hermes/Codex unavailable. |
| `inputCostUsd` | usd | P/O latest `usage.cost.input`, estimated. | Unavailable: no public aggregate component counter. |
| `outputCostUsd` | usd | P/O latest `usage.cost.output`, estimated. | Unavailable: no public aggregate component counter. |
| `cacheReadCostUsd` | usd | P/O latest `usage.cost.cacheRead`, estimated. | Unavailable: no public aggregate component counter. |
| `cacheWriteCostUsd` | usd | P/O latest `usage.cost.cacheWrite`, estimated. | Unavailable: no public aggregate component counter. |
| `premiumRequests` | premium-requests | P/O latest `usage.premiumRequests` only if provider actually supplies it; not inferred from requests. | O aggregate `premiumRequests`; other aggregates unavailable. |
| `creditCost` | provider-defined | P/O latest optional `usage.credits.cost`; native provider units retained. | Unavailable: aggregate getter exposes no credit counter. |
| `committedCreditCost` | provider-defined | P/O latest optional `usage.credits.committedCost`; native provider units retained. | Unavailable: aggregate getter exposes no committed-credit counter. |
| `acuCost` | provider-defined | P/O latest optional `usage.credits.acuCost`; native provider units retained. | Unavailable: aggregate getter exposes no ACU counter. |

Claude, Codex and Hermes provide none of the above latest component/premium/credit/ACU cost counters in implemented projections. Optional provider fields absent on a particular Pi/OMP version/provider remain unavailable, not zero.

### Activity and timing

| Exact metric | Unit / scope | Actual source and unsupported status |
| --- | --- | --- |
| `userMessageCount` | count | Unavailable in all implemented adapters; no public native total used, no history/event approximation. |
| `assistantMessageCount` | count | Unavailable in all implemented adapters; no public native total used. |
| `toolCallCount` | count | Unavailable in all implemented adapters; current tool metadata is not a cumulative counter. |
| `toolResultCount` | count | Unavailable in all implemented adapters; tool results/history are not read. |
| `totalMessageCount` | count | Unavailable in all implemented adapters; no public native total used. |
| `pendingMessageCount` | count | Unavailable in all implemented adapters; no queued-prompt text or approximated count. |
| `pendingAsyncWorkCount` | count / native-aggregate snapshot | O async `delivery.queued`: native queued plus in-flight deliveries; `delivering` is a boolean, never added as another count. Other harnesses unavailable. |
| `activeAsyncJobCount` | count / native-aggregate snapshot | Derived length of O async `running` only, not recent jobs plus deliveries. Other harnesses unavailable. |
| `routedModelCounts` | model/provider rows with count metric | Unavailable in all implemented adapters: public context has no finalized native routed-model counter. Model metadata is not a routed count; no event approximations. |
| `sessionRunningMs` | milliseconds / native-aggregate | C status `cost.total_duration_ms`: native accumulated running duration across resumes, excluding time not running. All other implemented adapters unavailable. |
| `apiWaitMs` | milliseconds / native-aggregate | C status `cost.total_api_duration_ms`: native accumulated API duration. All other implemented adapters unavailable; hook wait state is not a duration counter. |

No host-wide disk/network/GPU/heap telemetry, account spend limits or additional numeric metrics are collected. Non-numeric goals/todos/activity are presence metadata only where a native source supplies them.

## Native API/version evidence

Actual source is authoritative; external public APIs can change independently of the lockfile. Optional getters/fields are not claims that every older installed host implements them.

- **Effect 4.0.3**: installed [`Context.Service`](../node_modules/effect/src/Context.ts), [`Layer.effect`](../node_modules/effect/src/Layer.ts), [`Schema.TaggedError`](../node_modules/effect/src/Schema.ts), and [`ai/McpServer`](../node_modules/effect/src/ai/McpServer.ts). Corresponding pinned public source: [Context](https://github.com/Effect-TS/effect/blob/effect%404.0.3/packages/effect/src/Context.ts), [Layer](https://github.com/Effect-TS/effect/blob/effect%404.0.3/packages/effect/src/Layer.ts), [Schema](https://github.com/Effect-TS/effect/blob/effect%404.0.3/packages/effect/src/Schema.ts), [McpServer](https://github.com/Effect-TS/effect/blob/effect%404.0.3/packages/effect/src/ai/McpServer.ts). These installed v4 APIs, not stale `Layer.scoped` / `TaggedErrorClass` examples, define the implementation. Runtime dependencies are only effect 4.0.3, @effect/platform-node 4.0.3 and @modelcontextprotocol/client 2.3.1; native SDKs are development/host dependencies.
- **Pi 1.0.4**: installed [public extension types](../node_modules/@earendil-works/pi-coding-agent/dist/core/extensions/types.d.ts), notably `getContextUsage`; latest assistant usage is projected by [plugin.ts](../src/plugin.ts). No public session aggregate is assumed, no `getEntries` scan/full SessionStats handle. Context source may be unknown after compaction.
- **OMP**: installed [extension types](../node_modules/@oh-my-pi/pi-coding-agent/src/extensibility/extensions/types.ts) expose `getContextUsage` / `getAsyncJobSnapshot`; [SessionManager](../node_modules/@oh-my-pi/pi-coding-agent/src/session/session-manager.ts) exposes read-only `getUsageStatistics()` returning the native indexed snapshot. Package development pins remain 18.6.1; actual Linux native CLI smoke used compiled 18.11.3. Public finalized indexed totals include completed delegated spend as documented above, not a branch/history reconstruction. Native keyword registration is replace-only, not a general third-party registry merge.
- **Hermes**: native [shell-hook documentation](https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks#shell-hooks), inspected installed-Mac revision [`acc3cada0da8` shell_hooks.py](https://github.com/NousResearch/hermes-agent/blob/acc3cada0da8/agent/shell_hooks.py), and the [implemented TypeScript command-hook boundary](../src/native.ts). This dispatcher parses argv and directly invokes `subprocess.Popen` with `shell=False`; the generated env-to-Node command preserves the actual shared dispatcher-host parent. This is the supported source boundary, not the retired PluginContext integration or a claim that every stable Hermes release supports it.
- **Claude Code**: public [hooks](https://code.claude.com/docs/en/hooks), [statusline payload/semantics](https://code.claude.com/docs/en/statusline), native `claude agents --json` roster when supported, and [implemented bounded projectors/roster](../src/native.ts). Preserve custom statusLine via optional manual composition. Native current-context token figures must not be labeled lifetime; native client-estimated cost and durations keep their semantics. Roster PID identity is independently checked; hooks do not fabricate process IDs.
- **Codex**: public [app-server documentation](https://developers.openai.com/codex/app-server), [native protocol source](https://github.com/openai/codex/tree/main/codex-rs/app-server-protocol), and [implemented v2 ingestion](../src/native.ts). Accept only the versioned `attached-app-server` envelope for `thread/tokenUsage/updated`; ordinary CLI hooks contribute metadata. No arbitrary-TUI attach, extra SDK session, hidden server or invented per-thread USD cost. Native total/cache/reasoning overlap is preserved.

## Evidence and release boundary

Current parent-observed checks and precise limits are recorded once in [README: observed cutover evidence](../README.md#observed-cutover-evidence-and-limits): current build/check/test/smoke passed with **six suites / 52 tests**. Hermes end-to-end proof exercises the actual built CLI, private cache, outbound connector and authenticated MCP using **explicitly synthetic native envelopes**, not live provider callbacks. The README remains the single acceptance source, distinguishing prior Pi/OMP CLI/TUI, stdio, installer, hardened-image, Helm and Darwin evidence from newly exercised checks.

These disposable proofs do not establish physical cross-machine TLS deployment, live provider callbacks, signed publication or production activation. The README records the observed installed-Mac native command-hook and hardened-image checks; obsolete Python plugin registration is not evidence for this boundary. No global native installation/settings change, live stop, durable-state deletion/import or commit was performed. Historical execution-era 123/128/132-test claims are not current inspector evidence.

The baked image identity is `COORDINATOR_IMAGE_SHA`; CI build-push passes actual `github.sha`. Helm/readiness and exact-image smoke compare the separate `COORDINATOR_EXPECTED_SOURCE_SHA` to `x-coordinator-source-sha`, never override the baked value. Exact-source green CI, immutable digest, expected signer, SBOM/SLSA and reviewed GitOps promotion remain required; no current publication/provenance/deployment gate is implied by source syntax/hash checks or local smoke.
