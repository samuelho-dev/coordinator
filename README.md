# Coordinator

A lightweight, read-only inspector for **existing native Pi, OMP, Hermes, Codex CLI and Claude Code sessions** on Linux/macOS. Native agents retain their conversations, execution, approvals and lifecycle; Coordinator publishes bounded presence and answers on-demand counter/resource queries without creating another agent session.

## Workflow and boundaries

1. An explicitly configured native extension or metadata connector announces existing sessions to an authenticated, ephemeral inspector registry. Connectors initiate outbound HTTP; a Mac terminal needs no inbound listener.
2. Same-owner connected agents use **`session.list`** for presence and **`session.inspect`** for one session's native counters and attributable process resources. These are the entire model-facing tool catalog, both read-only. Other owners cannot read the sessions; publishers can write only their authenticated machine's observations.
3. Presence repeats every 10 seconds. Native detail getters and an approximately 200 ms OS CPU/RSS sample run only on demand. Connector announce/poll/result/disconnect are bounded transient read RPCs, not durable jobs. Registry restart loses discovery until reannouncement, never native work.
4. In OMP, a leading `coordinate` word transforms the **current input** with a real presence snapshot and read-only inspection cue. It is not a role, forwarding operation or executor. Other input is unchanged. Native keyword highlighting preserves enabled core words and the custom editor; the replace-only keyword API cannot enumerate arbitrary third-party registrations, so composition with those registrations is not guaranteed.

Rex remains the terminal owner; Hermes workflows/cron, native CI and collaboration remain external execution authorities. Coordinator has no task board, queue/executor, broker/runner, role/lease/handoff, background model, independent pricing engine, account quota subsystem, transcript archive or tool-result history. Inspection never reads queued prompt text, conversations, system prompts or tool inputs/results.

## Source

There are **seven handwritten `src/` files** and only two Effect application service owners: `Inspector` and `Mcp`. Imports are inert; native projections and resource collectors are plain functions, not additional service owners.

```text
src/
├── inspection.ts   # boundary schemas, metric/provenance contracts, two tool descriptors
├── resources.ts    # Linux/Darwin process identity and on-demand resource sampling
├── inspector.ts    # Inspector: authenticated ephemeral registry and read RPC owner
├── mcp.ts          # Mcp: bounded HTTP/MCP client/server and stdio transport
├── native.ts       # native projections, private metadata cache, outbound connectors
├── plugin.ts       # actual-session Pi/OMP observer and extension entrypoint
└── main.ts         # CLI and root composition
scripts/
├── install.ts      # explicit native-host artifact installer
├── smoke.ts        # disposable native CLI/HTTP/connector smoke
└── deployment-smoke.ts # exact-image authenticated inspector smoke
```

| File / owner | Callable surface and responsibility |
| --- | --- |
| [inspection.ts](src/inspection.ts) | `SessionRef`, presence/metric/observation/config schemas, strict `decode*` boundaries; `sessionRefKey`, `metric`, `unavailableMetric`, `createInspection`; `InspectionTools.list` / `.inspect` descriptors. `HttpError` uses `Schema.TaggedError`; no layer/state owner. |
| [inspector.ts](src/inspector.ts) / `Inspector` | Scoped `make(config)` and `layer(config)`; `authorize(token)`, `list(principal,input)`, `inspect(principal,input)`, `announce(principal,input)`, `poll(principal,input)`, `complete(principal,input)`, `disconnect(principal,input)`. Typed Effects with `HttpError`; receiver clock owns liveness. No database or native execution. |
| [mcp.ts](src/mcp.ts) / `Mcp` | `make`, `layer`; generic `call(url,token,descriptor,params,timeoutMs?)`, typed `post(url,token,path,inputSchema,outputSchema,params,timeoutMs?)`, scoped `serve(server,inspector)`, generic `serveStdio(reader)` with `list(input)` / `inspect(input)` Effects. `serverLayer(host,port)` supplies the Node HTTP server. |
| [resources.ts](src/resources.ts) | `identifyProcess(pid)`, `observeResources(roots)`, `parseDarwinSnapshot(output)`; process birth/parent identity and bounded resource sampling. `ResourceError`; no service/layer. |
| [native.ts](src/native.ts) | `readPrivateFile`, `loadConnectorConfig(file?)`, `readConnectorToken`, scoped `connectNative(config,{presence,inspect})`, `withProcessResources`; `projectHook`, `projectClaudeStatus`, `projectCodexNotification`, `writeHermesHook(config,envelope,at=Date.now(),hostPid=process.ppid)`; `writeNativeCache` / `readNativeCache`, `claudeRoster(config)`, `connectExternal(config)`. `NativeError`; no native SDK/session owner. |
| [plugin.ts](src/plugin.ts) | `createNativeObserver(harness,machineId,processes?)` returns `update`, `presence`, `inspect`, `clear`; retains current native context, scalar latest usage and active tool identifiers, not message/event history. `isCoordinateInput`, `transformCoordinateInput`, `registerCoordinateKeyword`; default extension registers lifecycle/input hooks and a scoped outbound connector through Node/Mcp ManagedRuntime. |
| [main.ts](src/main.ts) | `cli(args)` scopes modes. Inspector mode composes `Inspector.make`, `serverLayer`, `Mcp.serve`; connector uses `connectExternal`; MCP mode supplies a remote descriptor reader to `serveStdio`; list/inspect use those same descriptors; native stdin adapters project/cache bounded metadata. Executable root provides `Layer.mergeAll(NodeServices.layer,Mcp.layer)` through `NodeRuntime.runMain`. |

`Inspector.inspect` forwards one read request to the current connector incarnation; timeout/cancellation removes it. Bearer-authenticated `/connector/announce`, `/connector/poll`, `/connector/result` and `/connector/disconnect` are transport endpoints, not model tools. `/mcp` exposes the two tools with read-only hints. Receiver liveness is 30 seconds, discovery expiry 120 seconds, registry query deadline 10 seconds and detail callback deadline 8 seconds. HTTP/native JSON input is bounded to 64 KiB; detail is bounded to 48 KiB with explicit partial flags and retained aggregate scope. Framing/SSE output is bounded; redirects/token forwarding and unsafe Origins are rejected.

`prisma/` and `migrations/` are historical rollback source assets, **not** current database dependencies or schema setup. There is no automatic historical import or deletion; see [deployment and rollback](#deployment-and-rollback).

### Build and entrypoints

Use the pinned lockfile and an authorized exact-source checkout. The package declares Node **>=22.19** and pnpm **11.9.0**; the production image uses Node **24.21.0**. Pi is pinned to **1.0.4**, the OMP development SDK to **18.6.1**, and Effect to **4.0.3**. Source-package OMP requires **Bun >=1.3.14** (CI pins **1.4.2**); do not load its SDK as a Node SDK or replace global binaries/profiles. A compiled OMP executable has its own runtime/version boundary; the observed native smoke used compiled **18.11.3**, not the pinned SDK artifact.

```sh
pnpm install --frozen-lockfile
pnpm build
```

`build` clears generated `dist/`, patches the installed TypeScript integration, emits ESM/declarations for `src/` and the three scripts, and creates **`dist/plugins/pi.js`** and **`dist/plugins/omp.js`**. Host manifests select their corresponding bundle. Native SDKs are host/development dependencies, not inspector runtime dependencies. The CLI is **`dist/src/main.js`** (package bin `coordinator`), not the retired `dist/agent/` distribution.

| Package command | Exact entrypoint / purpose |
| --- | --- |
| `pnpm inspector` | `node dist/src/main.js inspector`: ephemeral HTTP registry, private server config through `COORDINATOR_CONFIG`. |
| `pnpm connector` | `node dist/src/main.js connector`: outbound external-native metadata/roster publisher. |
| `pnpm mcp` | `node dist/src/main.js mcp`: stdio bridge to the same two remote read-only tools. |
| `pnpm install:host` | `node dist/scripts/install.js`: explicit artifact generation; manual native activation below. |
| `pnpm smoke` | `node dist/scripts/smoke.js`: disposable native CLI/HTTP/connector smoke, not deployment. |
| `pnpm check` | `tsc --noEmit`; no database generation/URL. |
| `pnpm test` | `vitest run --configLoader native`; no PostgreSQL prerequisite. |

Additional CLI modes are `help` / `--help` (also the no-argument default), `list`, `inspect`, `hook claude|codex|hermes`, `claude-statusline` and `codex-notification`. `inspect` consumes bounded JSON on **stdin**, not a session-ID flag. Hook/statusline/notification adapters whitelist metadata into private cache, produce no display output and never echo input/secrets. Ordinary command failures exit **1**; native observer failures report redacted errors to stderr and exit **0**, so inspection cannot block native work.

## Configuration

[inspection.ts](src/inspection.ts) defines **different, strict JSON shapes** for server and connector; unknown properties are rejected. [native.ts](src/native.ts) owns private-file checks. Old `plugin.json`, database URLs, repository enrollment, execution roles and model credentials are not current interfaces.

Use separate bearer tokens for each owner/machine principal. Each enrollment has `id`, `owner`, `machineId` and a lowercase SHA-256 `tokenHash`. Same-owner principals can read one another's observations; publishing is restricted to the authenticated machine. **There is no read-only token role.** Protect every enrolled token accordingly. Hash the exact token bytes consumed after trimming; token files accept 16–4096 non-whitespace characters.

Run as the actual nonroot native host owner. Config/token files must be owner-owned regular files with no group/other permissions (**0600**) in an owner-owned canonical immediate parent with no group/other permissions (**0700**). Reads reject final symlinks, noncanonical paths (including `..`) and files over 64 KiB. Protect the state directory the same way; use a new directory, not an old runtime store. It holds bounded metadata, not conversations or task journals.

### Private bootstrap

Keep shell tracing off. Select a **new canonical absolute directory** and adapt this example to the actual owner/OS. It refuses an existing directory and exclusively creates token/hash files; raw tokens never enter argv, environment, logs or Git.

```sh
(
  set -eu
  umask 077
  export PRIVATE=/home/operator/.config/coordinator-inspector-private
  mkdir -m 700 "$PRIVATE"
  node --input-type=module <<'JS'
import { randomBytes, createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
const token = randomBytes(32).toString('hex');
writeFileSync(`${process.env.PRIVATE}/machine.token`, token, { mode: 0o600, flag: 'wx' });
writeFileSync(`${process.env.PRIVATE}/machine.sha256`, createHash('sha256').update(token).digest('hex'), { mode: 0o600, flag: 'wx' });
JS
  mkdir -m 700 "$PRIVATE/state"
)
```

Using protected editor/secret tooling, create **new** owner-only files below; do not overwrite existing configuration. Copy only the generated hash into enrollment and replace `REPLACE_WITH_64_LOWERCASE_HEX_SHA256` before use. The example contains no real credential. Provision raw tokens only through private files/your secret backend, never native MCP settings or plaintext Git.

**Server** `/home/operator/.config/coordinator-inspector-private/server.json`:

```json
{
  "host": "127.0.0.1",
  "port": 8787,
  "principals": [
    {
      "id": "workstation-publisher",
      "owner": "example-owner",
      "machineId": "example-workstation",
      "tokenHash": "REPLACE_WITH_64_LOWERCASE_HEX_SHA256"
    }
  ]
}
```

**Connector/client** `/home/operator/.config/coordinator-inspector-private/inspector.json`:

```json
{
  "inspectorUrl": "http://127.0.0.1:8787",
  "tokenFile": "/home/operator/.config/coordinator-inspector-private/machine.token",
  "machineId": "example-workstation",
  "stateDirectory": "/home/operator/.config/coordinator-inspector-private/state"
}
```

For another machine, enroll a distinct token/principal with the same owner and its own machine ID. Use real private HTTPS DNS and OS-local paths; do not copy Linux paths unchanged to a Mac. TLS is required except loopback (`localhost`, `127.0.0.1`, `[::1]`); URLs cannot contain credentials, query or fragment. Preserve system DNS and certificate-chain/hostname validation; no custom relay or verification bypass. If needed, supply a trusted public CA bundle through `NODE_EXTRA_CA_CERTS`, restarting Node after trust rotation. Never distribute a CA private key.

The connector defaults to `$XDG_CONFIG_HOME/coordinator/inspector.json`, otherwise `~/.config/coordinator/inspector.json`; `COORDINATOR_CONNECTOR_CONFIG` selects an explicit file. Missing default config disables native extension enrollment; an explicit missing, invalid or insecure config fails. CLI reader/connector modes require configuration. Never point the connector at server JSON. Server mode requires explicit `COORDINATOR_CONFIG`.

### Foreground commands

Select the same private directory in each shell; these commands do not install services:

```sh
export PRIVATE=/home/operator/.config/coordinator-inspector-private
COORDINATOR_CONFIG="$PRIVATE/server.json" node dist/src/main.js inspector
```

In another shell:

```sh
export PRIVATE=/home/operator/.config/coordinator-inspector-private
export COORDINATOR_CONNECTOR_CONFIG="$PRIVATE/inspector.json"
node dist/src/main.js help
node dist/src/main.js list
node dist/src/main.js inspect <<'JSON'
{"ref":{"machineId":"example-workstation","harness":"pi","nativeSessionId":"COPY_EXACT_ID_FROM_LIST"}}
JSON
```

Copy the **complete `ref`** from `list`, including `nativeAgentId` when supplied. CLI `list` requests up to 100 entries; native MCP `session.list` accepts its schema's pagination/filter inputs. `node dist/src/main.js mcp` is a stdio bridge for a native MCP launcher, not an interactive prompt.

Pi/OMP extensions connect directly from their actual sessions. Claude/Codex/Hermes hooks need a separately started external connector as the same owner:

```sh
COORDINATOR_CONNECTOR_CONFIG="$PRIVATE/inspector.json" node dist/src/main.js connector
```

## Native host setup

Build first and check installed native versions/API availability before activation. The installer creates a **new directory beneath an existing private owner directory**, validates private config/token access, and refuses root/existing destinations. It never overwrites settings, hooks, profiles, statusLine or credentials; it does not reload a TUI, start a connector, install remotely or manage services. `--runtime /absolute/path/to/node` optionally selects Node (default: the installer's executable). Generated artifacts refer to exact-source absolute paths: do not move/delete that build afterward.

Use a separate new destination per target. `--dry-run` validates prerequisites and prints activation instructions without writing:

```sh
pnpm install:host --target pi --destination "$PRIVATE/pi-host" --config "$PRIVATE/inspector.json" --dry-run
pnpm install:host --target pi --destination "$PRIVATE/pi-host" --config "$PRIVATE/inspector.json"
pnpm install:host --target omp --destination "$PRIVATE/omp-host" --config "$PRIVATE/inspector.json"
pnpm install:host --target claude --destination "$PRIVATE/claude-host" --config "$PRIVATE/inspector.json"
pnpm install:host --target codex --destination "$PRIVATE/codex-host" --config "$PRIVATE/inspector.json"
pnpm install:host --target hermes --destination "$PRIVATE/hermes-host" --config "$PRIVATE/inspector.json"
```

Every target includes private `connector.sh` and `mcp.sh` launchers. Review the printed instructions and manually activate only intended entries, preserving existing native hooks/settings/MCP servers:

| Harness | Generated artifacts and manual activation |
| --- | --- |
| Pi | Run the printed `pi install <built-package-root>` command; the real package's relative `pi` entry loads `dist/plugins/pi.js`. Native `coordinator_session_list` and `coordinator_session_inspect` tools use the authenticated read-only API without a Pi MCP adapter/config. Launch with `COORDINATOR_CONNECTOR_CONFIG`; native trust/reload requirements still apply. |
| OMP | Run the printed `omp plugin link <built-package-root>` command; the real package's relative `omp` entry loads `dist/plugins/omp.js`. Merge only the coordinator entry from generated `mcp.json` into selected `~/.omp/agent/mcp.json`; launch with the explicit connector-config environment. No wrapper plugin package is generated. |
| Claude Code | `.claude-plugin/plugin.json`, `hooks/hooks.json`, `.mcp.json`; launch `claude --plugin-dir "$PRIVATE/claude-host"`, approve the read-only MCP tools/metadata hooks, and separately start generated `connector.sh` as the same owner. Existing statusLine remains untouched. |
| Codex CLI | `hooks/hooks.json`, `mcp.toml`; merge generated hook entries into selected `~/.codex/hooks.json` or project `.codex/hooks.json`, and only `[mcp_servers.coordinator]` plus its env table into selected `config.toml`. Preserve all other entries/tables, approve native hooks and separately start generated `connector.sh`. |
| Hermes | `hooks.yaml` (JSON-compatible YAML), `mcp.yaml`, equivalent `mcp.json`; append only generated observer entries to `hooks:` in the selected profile's `config.yaml`, and merge only `mcp_servers.coordinator` from `mcp.yaml` into the same file. Preserve everything else, review/approve the intended native commands and separately start generated `connector.sh`. No plugin install/enable step or profile overwrite. Reader-only Hermes can merge just the MCP entry and omit observation hooks/connector. |

The generated Claude/Codex hooks cover `SessionStart`, `SessionEnd`, `UserPromptSubmit`, `PreToolUse`, `PostToolUse`, `PermissionRequest` and `Stop`; Claude also includes `PostToolUseFailure`, `PostModelSwitch`, `PreCompact`, while Codex includes `Interrupt`. They use bounded metadata projections, not prompt/tool payload archival.

### Hermes native hooks

Hermes uses the **shared TypeScript CLI**, `node dist/src/main.js hook hermes`, not an in-process Python plugin or separate Coordinator runtime. Upstream Hermes retains its own native runtime; Coordinator adds no Python dependency or `HERMES_HOME/plugins` installation.

Exactly four native events are supported:

- `on_session_start` establishes the record.
- `post_api_request` updates latest successful request usage, not account spend or lifetime totals.
- `on_session_end` means idle/turn end, not removal.
- `on_session_finalize` closes the old session, preferring `extra.old_session_id`; late callbacks cannot recreate a closed/absent record.

The supported native dispatcher parses argv and executes directly (`shell=False`). Generated commands use `/usr/bin/env COORDINATOR_CONNECTOR_CONFIG='<config>' '<node>' '<main>' hook hermes`; `env` replaces itself with Node, preserving the actual in-process native dispatcher parent PID. That verified PID plus OS birth identity is a **shared hook host**, never an exclusive session process, wire-supplied PID or guessed TUI ancestor. While the exact host lives, cache reads refresh presence only, not native activity or usage ages; dead/reused hosts are absent. Without verified host identity, metadata lasts at most 120 seconds and process/resources remain unavailable. There is no heartbeat daemon, periodic resource sampler or extra model session. Unknown/legacy cache files are retained, not migrated/deleted.

### Claude and Codex optional sources

Claude's connector can use native `claude agents --json` for independently checked PID roster identity when supported; ordinary hooks supply metadata. For optional metrics, manually compose the existing status-line command to pass the **same stdin JSON** to `node /absolute/coordinator/dist/src/main.js claude-statusline` with the connector-config environment, preserving existing display output. Never replace custom statusLine configuration; this adapter displays nothing. Current-context/latest-response tokens are not lifetime totals; context input/cache basis excludes output and cost is the native client estimate.

Codex ordinary hooks provide metadata, not invented PID/token counters. Only an **already attached native app-server source** may feed bounded stdin to `node dist/src/main.js codex-notification`: envelope `version: 2`, `source: "attached-app-server"`, `method: "thread/tokenUsage/updated"`, with native thread/turn/tokenUsage params. It does not launch an app-server, new thread or hidden SDK session, nor observe unrelated TUIs. Without that real source, counters remain unavailable.

## Metrics and provenance

Metrics report only native fields and attributable OS measurements that are actually available; missing/unsupported values are unavailable, never fabricated zeroes. Source, scope, freshness, estimate/shared/partial flags and aggregation boundaries accompany values; a schema field is not a claim that every harness supplies it.

### Identity, availability and freshness

[`inspection.ts`](src/inspection.ts) defines the complete numeric allowlist below; [`plugin.ts`](src/plugin.ts) and [`native.ts`](src/native.ts) supply native projections. `SessionRef` is `machineId + harness + nativeSessionId`, plus optional actual `nativeAgentId` to distinguish actors when supplied. Presence may include native parent/label/kind, provider/model/profile/agent, cwd/repository, Rex identifiers, machine/OS/version, PID/birth/ownership, activity/wait/approval/streaming/compaction/current tool, and bounded goals/todos. These are conditional native metadata, not a promise that every adapter collects every schema field. Pi/OMP expose current model/cwd/activity, OMP actual agent identity, and available Rex session/block environment metadata; external adapters project whitelisted fields only. No prompts, conversation, queued text, system prompt or tool input/results are transmitted.

Each numeric metric is either `available` with value/unit, or `unavailable` with null/reason; missing is never zero. `routedModelCounts` uses `values` rather than a scalar `value`. Provenance contains `source`, `scope`, `scopeDescription`, `observedAt`, nullable `nativeUpdatedAt`, `estimated`, `derived`, `shared`, `partial`, `stale`, and optional `sampleWindow` (`startedAt`, `endedAt`). `queriedAt` is the inspection time, not the native counter update time. Cached counters keep their original observation/native timestamps; unknown update times stay null.

Source-host presence timestamps, native activity and counter freshness are separate from receiver-clock connection receipt/liveness (`receivedAt`, `liveUntil`). Presence repeats every 10 seconds; receiver liveness is 30 seconds and registry expiry 120 seconds. A live connector does not prove a metadata-only Codex process is alive. Hermes cache reads refresh only presence `observedAt` while the exact bound host identity remains alive, not usage/native-update/activity timestamps. Without a host identity, Hermes has no process attribution and metadata expires after 120 seconds; a dead/reused bound host is absent from reads immediately. See the [registry](src/inspector.ts) and [native cache](src/native.ts).

All getters and optional fields are conditional on the actual installed host/provider. No transcript reconstruction, full SessionStats/history scan, event-accumulated lifetime estimates, independent ratecard pricing or parent/child sum fills missing counters.

### Source legend

| Alias | Native source and scope |
| --- | --- |
| OS | [`resources.ts`](src/resources.ts): Linux `/proc/<pid>/stat`, boot identity and native `getconf` units; Darwin `/bin/ps -axo pid=,ppid=,lstart=,time=,rss=`. Two on-demand snapshots approximately 200 ms apart, validated ancestry/birth; no command lines or host totals. [`withProcessResources`](src/native.ts) projects the measurements. |
| P/O context | Pi/OMP public `ctx.getContextUsage()`: `contextWindow`, nullable `tokens`/`percent`; estimated `current-context`. Unknown after compaction stays unknown. |
| P/O latest | Whitelisted latest native assistant `usage`, `latest-native-update`, original native timestamp if supplied. Native client costs are estimates, not a new price engine. |
| O aggregate | OMP optional `ctx.sessionManager.getUsageStatistics()`, `native-aggregate`: finalized cached indexed session entries, not selected branch or assumed lifetime. Total cost includes completed direct-child spend; never add child cost again. No aggregate reasoning/component-cost/credit counters. |
| O async | OMP optional `ctx.getAsyncJobSnapshot()`: current-session native snapshot; no job commands/labels read. |
| H latest | Successful Hermes `post_api_request` hook `extra.usage`, `latest-native-update`, not a session/account aggregate. Unknown native update time stays null. Input/output aliases take the first non-null supplied value, preserving zero; a later request missing usage does not reuse older request counters. Lifecycle metadata updates preserve latest-request counters and their original freshness. |
| C status | Optionally manually composed Claude statusline: token/context figures are current-context/latest-response, not lifetime totals. Native client session cost is estimated; native duration semantics retained. |
| X attached | Codex v2 `thread/tokenUsage/updated` from an already attached app-server: `last` → latest update, `total` → native attached-thread aggregate. Ordinary hooks are metadata only; no arbitrary-TUI observer, hidden app-server, fabricated PID or thread USD cost. |

### Process resources

PID plus birth protects attribution/reuse: Linux boot UUID plus start ticks; Darwin C-locale UTC `lstart` has one-second resolution, so same-second PID reuse cannot be distinguished. The registry resolves competing process claims without double owned charging. Shared/unknown processes may have per-process measurements but no fabricated owned aggregate; OMP and Hermes host PIDs are shared. OS projections retain actual observation times/windows with `derived:true`, `nativeUpdatedAt:null`. Unsupported, inaccessible, exited or reused processes are unavailable, not zero. Truncated details mark partial and retain the actual aggregate scope rather than recomputing from displayed rows.

| Exact metric | Unit / scope | Source and limits |
| --- | --- | --- |
| `cpuPercent` | percent-one-core / process sample | OS CPU-time delta divided by actual monotonic sample interval; 100% means one core. Requires a stable readable identified process/sample. |
| `cpuTimeMs` | milliseconds / process | OS cumulative process CPU time, not session running time; derived native unit conversion. Same identity/readability limits. |
| `rssBytes` | bytes / process | Linux resident pages × page size; Darwin RSS KiB × 1024. Not unique physical RAM. |
| `ownedProcessCount` | count / owned-processes | Unique exclusively attributable roots/descendants; unavailable on partial/truncated/shared/unknown attribution or absent roots, not invented zero. |
| `ownedCpuPercent` | percent-one-core / owned-processes | Unique exclusively attributable OS CPU sum; may exceed 100%. Shared/unknown attribution makes the aggregate unavailable; partial flags retained. |
| `ownedRssBytes` | bytes / owned-processes | Unique exclusively attributable RSS sum, not unique physical RAM. Shared/unknown attribution makes the aggregate unavailable; partial flags retained. |

### Context

| Exact metric | Unit / scope | Native source and limits |
| --- | --- | --- |
| `contextWindowTokens` | tokens / current-context | P/O context `contextWindow`; C status `context_window.context_window_size`; X attached non-null `modelContextWindow`. Hermes unavailable. |
| `contextUsedTokens` | tokens / current-context | P/O context known `tokens`; C status derived only with all current input + cache-read + cache-creation operands, excluding output. Codex/Hermes unavailable: no compatible input-only used basis. |
| `contextRemainingTokens` | tokens / current-context | Derived `max(0,window-used)` for known compatible P/O (positive window) or C status operands. Codex/Hermes unavailable. |
| `contextUsedPercent` | percent / current-context | P/O context `percent`; C status native `used_percentage`. Preserve over-limit values. Codex/Hermes unavailable. |
| `contextRemainingPercent` | percent / current-context | P/O derived `100-percent` with valid positive window; C status native `remaining_percentage`. Codex/Hermes unavailable. |
| `systemPromptTokens` | tokens | Unavailable in all adapters: no public native breakdown; never read/tokenize system prompt text. |
| `systemToolsTokens` | tokens | Unavailable in all adapters: no public native breakdown counter. |
| `systemContextTokens` | tokens | Unavailable in all adapters: no public native breakdown counter. |
| `skillsTokens` | tokens | Unavailable in all adapters: no public native breakdown counter. |
| `messagesTokens` | tokens | Unavailable in all adapters: no public native breakdown counter; no history scan. |

### Tokens

Every name below occurs in both `tokens.latestNativeUpdate` and `tokens.nativeAggregate`. Latest values are not copied into aggregate. Cache may be a subset of input and reasoning a subset of output: never blindly sum fields to invent total. All units are `tokens`.

| Exact metric | Latest native update | Native aggregate / limits |
| --- | --- | --- |
| `inputTokens` | P/O latest `input`; H latest `input_tokens` then `prompt_tokens`; C status `context_window.total_input_tokens` (current-context/latest semantics); X attached `last.inputTokens`. | O aggregate `input`; X attached `total.inputTokens`. Pi/Hermes/Claude aggregate unavailable. |
| `outputTokens` | P/O latest `output`; H latest `output_tokens` then `completion_tokens`; C status `context_window.total_output_tokens` (not lifetime); X attached `last.outputTokens`. | O aggregate `output`; X attached `total.outputTokens`. Others unavailable. |
| `cacheReadTokens` | P/O latest `cacheRead`; H latest `cache_read_tokens`; C status `context_window.current_usage.cache_read_input_tokens`; X attached `last.cachedInputTokens`. | O aggregate `cacheRead`; X attached `total.cachedInputTokens`. Others unavailable. |
| `cacheWriteTokens` | P/O latest `cacheWrite`; H latest `cache_write_tokens`; C status `context_window.current_usage.cache_creation_input_tokens`; optional X attached `last.cacheWriteInputTokens`. | O aggregate `cacheWrite`; optional X attached `total.cacheWriteInputTokens`. Missing optional counters unavailable. |
| `reasoningOutputTokens` | Pi latest `reasoning`; OMP latest `reasoningTokens`; H latest `reasoning_tokens`; X attached `last.reasoningOutputTokens`, only if supplied. Claude unavailable. | X attached `total.reasoningOutputTokens`; O aggregate has no reasoning counter; other aggregates unavailable. |
| `totalTokens` | P/O latest `totalTokens`; H latest `total_tokens`; X attached `last.totalTokens`. Claude unavailable; no inferred sum. | O aggregate `totalTokens`; X attached `total.totalTokens`. Others unavailable. |

### Cost and provider usage

Every name below occurs in both `cost.latestNativeUpdate` and `cost.nativeAggregate`. Native client USD estimates are not invoiced spend. Ratecards are not actual usage counters; Hermes account usage is not session usage.

| Exact metric | Unit | Latest native update | Native aggregate / limits |
| --- | --- | --- | --- |
| `totalCostUsd` | usd | P/O latest `usage.cost.total`, estimated. Other latest scopes unavailable. | O aggregate `cost`, estimated, including completed direct-child spend; C status `cost.total_cost_usd`, client-estimated session cost. Pi/Hermes/Codex unavailable. |
| `inputCostUsd` | usd | P/O latest `usage.cost.input`, estimated. | Unavailable: no public aggregate component counter. |
| `outputCostUsd` | usd | P/O latest `usage.cost.output`, estimated. | Unavailable: no public aggregate component counter. |
| `cacheReadCostUsd` | usd | P/O latest `usage.cost.cacheRead`, estimated. | Unavailable: no public aggregate component counter. |
| `cacheWriteCostUsd` | usd | P/O latest `usage.cost.cacheWrite`, estimated. | Unavailable: no public aggregate component counter. |
| `premiumRequests` | premium-requests | P/O latest `usage.premiumRequests` only when provider-supplied; not inferred from requests. | O aggregate `premiumRequests`; other aggregates unavailable. |
| `creditCost` | provider-defined | P/O latest optional `usage.credits.cost`; retain native units. | Unavailable: no aggregate credit counter. |
| `committedCreditCost` | provider-defined | P/O latest optional `usage.credits.committedCost`; retain native units. | Unavailable: no aggregate committed-credit counter. |
| `acuCost` | provider-defined | P/O latest optional `usage.credits.acuCost`; retain native units. | Unavailable: no aggregate ACU counter. |

Claude, Codex and Hermes provide none of these latest component/premium/credit/ACU cost counters in implemented projections. Missing optional Pi/OMP host/provider fields remain unavailable, not zero.

### Activity and timing

| Exact metric | Unit / scope | Native source and limits |
| --- | --- | --- |
| `userMessageCount` | count | Unavailable in all adapters; no public native total used, no history/event approximation. |
| `assistantMessageCount` | count | Unavailable in all adapters; no public native total used. |
| `toolCallCount` | count | Unavailable in all adapters; current tool metadata is not cumulative. |
| `toolResultCount` | count | Unavailable in all adapters; tool results/history are not read. |
| `totalMessageCount` | count | Unavailable in all adapters; no public native total used. |
| `pendingMessageCount` | count | Unavailable in all adapters; no queued-prompt text or approximated count. |
| `pendingAsyncWorkCount` | count / native-aggregate snapshot | O async `delivery.queued`: queued plus in-flight deliveries; `delivering` is boolean, never added as another count. Other harnesses unavailable. |
| `activeAsyncJobCount` | count / native-aggregate snapshot | Derived length of O async `running` only; no recent jobs or deliveries added. Other harnesses unavailable. |
| `routedModelCounts` | model/provider rows with count metric | Unavailable in all adapters: no finalized public native routed-model counter. Model metadata is not a routed count; no event approximation. |
| `sessionRunningMs` | milliseconds / native-aggregate | C status `cost.total_duration_ms`: native accumulated running duration across resumes, excluding not-running time. Other adapters unavailable. |
| `apiWaitMs` | milliseconds / native-aggregate | C status `cost.total_api_duration_ms`: native accumulated API duration. Other adapters unavailable; hook wait state is not a duration counter. |

No host-wide disk/network/GPU/heap telemetry, account spend limits or additional numeric metrics are collected. Non-numeric goals/todos/activity remain conditional native presence metadata, not counters.

## Native API references

Implementation and installed-version APIs are authoritative; optional getters/fields are not attach guarantees for older hosts. Public upstream main branches may differ from pinned distributions.

- **Effect 4.0.3:** pinned public [Context](https://github.com/Effect-TS/effect/blob/effect%404.0.3/packages/effect/src/Context.ts), [Layer](https://github.com/Effect-TS/effect/blob/effect%404.0.3/packages/effect/src/Layer.ts), [Schema](https://github.com/Effect-TS/effect/blob/effect%404.0.3/packages/effect/src/Schema.ts) and [McpServer](https://github.com/Effect-TS/effect/blob/effect%404.0.3/packages/effect/src/ai/McpServer.ts). The implementation uses v4 `Context.Service`, `Layer.effect`, `Schema.TaggedError` and native MCP protocol adapters.
- **Pi:** [development SDK 1.0.4](https://www.npmjs.com/package/@earendil-works/pi-coding-agent/v/1.0.4) and [implemented native observer/readers](src/plugin.ts). `registerTool` readers derive their parameters from the shared Effect schemas and do not depend on native MCP support. `getContextUsage` and latest assistant usage are used without a history/SessionStats scan; context source can be unknown after compaction.
- **OMP:** [published 18.6.1 SDK](https://www.npmjs.com/package/@oh-my-pi/pi-coding-agent/v/18.6.1) and [implemented observer](src/plugin.ts) use native `getContextUsage`, optional `getAsyncJobSnapshot` and `SessionManager.getUsageStatistics()` indexed snapshots. Development SDK 18.6.1 and observed compiled host 18.11.3 are different artifacts. Finalized native totals include completed delegated spend; keyword registration is replace-only, not a third-party registry merge.
- **Hermes:** native [shell-hook documentation](https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks#shell-hooks), inspected [revision `acc3cada0da8` dispatcher source](https://github.com/NousResearch/hermes-agent/blob/acc3cada0da8/agent/shell_hooks.py), and [TypeScript command-hook implementation](src/native.ts). Installed Mac evidence used `0.22.1+233.gacc3cad`; this is not a compatibility claim for every stable Hermes release.
- **Claude Code:** native [hooks](https://code.claude.com/docs/en/hooks), [status-line payload/semantics](https://code.claude.com/docs/en/statusline), [CLI roster reference](https://code.claude.com/docs/en/cli-reference) and [implemented projectors/roster](src/native.ts). Optional status-line metrics retain native current-context, estimated-cost and duration scopes.
- **Codex:** native [app-server contract](https://developers.openai.com/codex/app-server/), [protocol source](https://github.com/openai/codex/tree/main/codex-rs/app-server-protocol) and [implemented v2 ingestion](src/native.ts). Native total/cache/reasoning overlap is preserved; no arbitrary-TUI attachment or invented per-thread USD cost.

## Deployment and rollback

Use the [chart operator README](deploy/helm/coordinator/README.md) for Kubernetes values, Secret keys, private staging, probes, retained legacy PVC declarations and release procedures. The chart supplies an inspector Deployment/internal Service, not a database, migration, agent runtime, controller, TLS ingress, DNS, NetworkPolicy or secret manager. Private TLS routing and policy remain operator-owned.

This is a breaking product/protocol cutover, not a task-store upgrade. Preserve **real databases, journals, backups, PVCs, old images and rollback coordinates**. Inventory actual live desired state and storage, drain old clients/work using old-release procedures, **fence old owners**, then explicitly change downstream routes from `<release>-broker` to `<release>-inspector` and reload selected clients. A new chart render, `Recreate` or retention annotation does not drain/fence owners. Do not overlay a fresh release onto a stateful old installation or let GitOps prune storage. The new CLI cannot import/recover old tasks; retired broker/runner/backup/remote-install commands are not aliases.

`/livez` is minimal public liveness; `/readyz` and `/healthz` require bearer authorization. Authenticated readiness returns baked `COORDINATOR_IMAGE_SHA` in `x-coordinator-source-sha`; chart `sourceSha` supplies separate `COORDINATOR_EXPECTED_SOURCE_SHA`. Never override baked image identity to make the comparison pass. Matching hash/header syntax is not provenance.

Promotion requires exact immutable-source CI, hardened exact-image smoke, verified digest/signature/expected OIDC producer, SBOM/SLSA repository/commit attestation, verified chart artifact and reviewed GitOps digest/source-SHA pins. Source/image/chart remain private pending a separate visibility/legal decision; read [third-party notices](THIRD_PARTY_NOTICES.md) before redistribution. Local proofs or GitOps health do not prove real private TLS/MCP/native-host acceptance. **Nothing here implicitly authorizes deployment, service stops, state migration or pruning.**

CI uses only repository-scoped homelab ARC pools: `coordinator-runners` for supplementary and chart checks, and `coordinator-dind-runners` for image publication and both release jobs. There is no GitHub-hosted fallback. PR jobs retain read-only contents permission without publish or OIDC credentials. Keep the private repository's server-side **Run workflows from fork pull requests** setting disabled, with fork write tokens and secrets disabled; same-repository PR source checks are defense in depth, not protection against a fork changing workflow YAML. The pinned runner image already includes Ruby/Psych, GitHub CLI and the native tools used by these workflows.

## Verification

After building, the normal local checks are:

```sh
pnpm check
pnpm test
pnpm smoke
```

`COORDINATOR_TEST_PI` and `COORDINATOR_TEST_OMP` may select existing absolute native CLI paths for smoke; do not upgrade global tools to match a disposable check.

Recorded implementation evidence (not a claim that these checks ran during this README edit):

- Final build/typecheck and **52 tests across six suites** passed. Real Linux disposable Pi **1.0.4** and compiled OMP **18.11.3** extension/HTTP/connector smoke exercised owner/machine isolation and on-demand OS resources with **zero prompts/provider calls**.
- Built Hermes TypeScript CLI/private-cache/connector/authenticated-MCP paths passed with **synthetic native envelopes**, including shared host identity/resources, zero/missing usage, privacy, turn end, finalize and late callbacks—not live provider callbacks.
- Official SDK stdio, all five installer targets (private artifacts, existing-destination refusal, no raw bearer), Darwin OS sampling, native OMP keyword/current-input TUI behavior and chart validation were exercised in disposable checks.
- Hardened image smoke used UID/GID 1000, all capabilities dropped, no-new-privileges, read-only root and `/tmp` tmpfs, including the synthetic Hermes path and baked/expected SHA comparison. Pi/OMP executables are intentionally absent from the image; real CLI smoke ran on the host.
- Installed Mac Hermes **`0.22.1+233.gacc3cad`** ran all four commands through actual `hooks doctor` with a disposable TypeScript build and **synthetic payloads**. Commands returned empty stdout/exit 0; observations checked shared dispatcher PID/birth, unknown start/API state, turn-idle/finalized state and unchanged usage ages. A config path containing spaces/apostrophe worked.

These are local disposable proofs, **not** live provider usage, physical cross-machine TLS acceptance, signed release/publication or production deployment. No global native settings change, live-service stop or historical state migration is implied.
