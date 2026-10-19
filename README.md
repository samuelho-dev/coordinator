# Coordinator

A lightweight, read-only inspector for **existing native Pi, OMP, Hermes, Codex CLI and Claude Code sessions** on Linux/macOS. Native agents keep their conversations, execution, approvals and lifecycle. Coordinator publishes bounded presence and answers on-demand counter/resource queries; it does not create another agent session.

## Expected flow and boundaries

1. An explicitly configured native extension or metadata connector announces existing sessions to one authenticated, ephemeral inspector registry. Connectors initiate outbound HTTP; a Mac terminal does not need an inbound listener.
2. Any same-owner connected agent can use **`session.list`** for basic presence and **`session.inspect`** for one session's native counters and attributable process resources. These are the entire model-facing tool catalog, both read-only. Other owners cannot read the sessions; publishers can write only their authenticated machine's observations.
3. Basic presence repeats every 10 seconds. Native detail getters and an approximately 200 ms OS CPU/RSS sample run only when requested. Connector announce/poll/result/disconnect are bounded transient read RPCs, not durable jobs. Restart loses registry discovery until reannouncement, never native work.
4. In OMP, a leading `coordinate` word transforms the **current input** with a real presence snapshot and a read-only inspection cue. It is not a role, forwarding operation or executor. Other input is unchanged. Native keyword highlighting preserves enabled core words and the custom editor, but the native replace-only keyword API cannot enumerate arbitrary third-party registrations, so composition with those registrations is not guaranteed.

Rex remains the terminal owner; Hermes keeps its workflows/cron; native CI and collaboration remain external authorities. Reuse those tools directly rather than cloning their control engines here. Coordinator has no task board, queue/executor, broker/runner, role/lease/handoff, background model, independent pricing engine, account quota subsystem, transcript archive or tool-result history. It never reads queued prompt text, conversations, system prompts or tool inputs/results for inspection.

## Source and metrics

There are **seven handwritten `src/` files**, with only two Effect application service owners (`Inspector`, `Mcp`). See the [complete implemented tree, every owner/method, native API evidence and complete numeric metric catalog](docs/src-consolidation-plan.md). That document is the single metric reference: a schema field is not a claim that every harness supplies it. Missing/unsupported values are unavailable, never fabricated zeroes.

Hermes uses the shared TypeScript CLI, `node dist/src/main.js hook hermes`, through native `hooks:` configuration—not an in-process Python plugin or separate Hermes product. Its supported command surface is documented in [native shell hooks](https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks#shell-hooks) and inspected at [upstream revision `acc3cada0da8`](https://github.com/NousResearch/hermes-agent/blob/acc3cada0da8/agent/shell_hooks.py); this is not a compatibility claim for every stable release. Pi's pinned host dependency is **1.0.4**, OMP's package/development dependency is **18.6.1**; the real Linux cutover smoke used Pi **1.0.4** and compiled OMP **18.11.3**. API availability is native-version conditional, not an attach guarantee for arbitrary installed TUIs.

Old execution runtime, generated runtime contracts and old execution tests/tooling have been removed. `prisma/` and `migrations/` remain historical rollback source assets, not dependencies or schema setup for this inspector. Real databases, journals, backups, PVCs and old images are preserved. There is no automatic historical import or deletion.

## Build and entrypoints

Use the pinned lockfile; do not replace global binaries or user profiles. Running the source-package OMP CLI requires Bun ≥1.3.14 (CI pins 1.4.2). `COORDINATOR_TEST_PI` and `COORDINATOR_TEST_OMP` can select existing absolute native CLI paths for `pnpm smoke`; the observed Linux smoke selected the installed compiled OMP, without upgrading the global Bun executable.

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm check
pnpm test
```

`build` clears generated `dist/`, patches the installed TypeScript integration, emits ESM/declarations for `src/` and the three scripts, then creates host-specific bundles **`dist/plugins/pi.js`** and **`dist/plugins/omp.js`**. Package manifests select the corresponding bundle. The CLI is **`dist/src/main.js`** (package bin `coordinator`), not the retired `dist/agent/` distribution.

| Package command | Exact entrypoint / purpose |
| --- | --- |
| `pnpm inspector` | `node dist/src/main.js inspector`: ephemeral HTTP registry; private server configuration via `COORDINATOR_CONFIG`. |
| `pnpm connector` | `node dist/src/main.js connector`: outbound external-native metadata/roster publisher. |
| `pnpm mcp` | `node dist/src/main.js mcp`: stdio bridge exposing the same two read-only remote tools. |
| `pnpm install:host` | `node dist/scripts/install.js`: explicit native-host installer; see operator documentation before use. |
| `pnpm smoke` | `node dist/scripts/smoke.js`: disposable native CLI/HTTP/connector smoke, not deployment. |
| `pnpm check` | `tsc --noEmit`; no database generation/URL. |
| `pnpm test` | `vitest run --configLoader native`; no PostgreSQL prerequisite. |

Additional CLI commands are `list`, `inspect` (bounded JSON input on stdin), `hook claude|codex|hermes`, `claude-statusline`, and `codex-notification`. Hook/statusline/notification input is whitelisted into private metadata cache; it produces no display output or echoed secret/input. Ordinary CLI failures exit 1; native observer hook failures are reported on stderr and exit 0 so inspection cannot block native work. Claude statusline support is optional **manual composition**, never replacement of a custom statusLine. Codex notification ingestion is only for a real attached app-server v2 source; it does not start a hidden app-server or observe unrelated TUIs.

Connector configuration is `$XDG_CONFIG_HOME/coordinator/inspector.json` (otherwise `~/.config/coordinator/inspector.json`), optionally selected by `COORDINATOR_CONNECTOR_CONFIG`. It contains `inspectorUrl`, `tokenFile`, `machineId`, `stateDirectory`. Missing default configuration disables the native extension; explicit missing, invalid or insecure configuration fails. Private owned directories/files and separate token files are required. Old `plugin.json`, database URL, runner and SDK-session configuration are not current interfaces. Detailed installation, authentication, Helm and release procedures: [self-hosting](docs/self-hosting.md) and [chart](deploy/helm/coordinator/README.md).

## Identity, freshness and trust

Identity is machine + harness + native session ID, plus a native agent ID only where supplied. PID attribution includes native birth identity; shared/unknown processes are explicit and never charged twice to owned totals. CPU 100% means one core; summed RSS is not unique physical RAM. Cached native counters retain their observation/update timestamps; reading now does not make them fresh. Native aggregate is not automatically lifetime/current-branch usage, cache/reasoning may already be included in input/output, and completed delegated spend must not be added again.

Receiver-clock connection liveness is separate from native activity/counter freshness: 30 seconds live, 120 seconds discovery expiry, 10-second registry query deadline. Connection health is not proof that a metadata-only Codex process is alive. Observations are bounded to 48 KiB, with explicit partial detail flags and preserved aggregate scopes.

Hermes binds the actual native dispatcher parent PID and OS birth identity as a **shared hook host**, never an exclusive session process, wire-supplied PID or guessed TUI ancestor. While that exact host lives, cache reads refresh presence only, not native activity or usage ages; a dead/reused host is absent. Without verified identity, metadata lasts at most 120 seconds and process/resources remain unavailable. Hooks run the shared CLI directly via `/usr/bin/env` → Node, with no shell intermediary, heartbeat daemon or extra model session. Unknown/legacy cache files are retained, not migrated or deleted.

TLS is required except loopback, with bearer authorization, Origin checks, bounded framing and redacted errors. `/livez` is minimal public liveness; `/readyz` and `/healthz` require authorization. Readiness reports the baked `COORDINATOR_IMAGE_SHA` as `x-coordinator-source-sha`; deployment expectations use separate `COORDINATOR_EXPECTED_SOURCE_SHA`, never override baked identity. Exact-source CI, immutable image identity, signatures/SBOM/SLSA and reviewed GitOps promotion remain release requirements; syntax/hash checks are not provenance.

## Observed cutover evidence and limits

The parent-observed current cutover passed `pnpm build`, `pnpm check` and **52 tests across six suites**. Real Linux disposable CLI smoke passed native Pi 1.0.4 and compiled OMP 18.11.3 extensions, authenticated HTTP, outbound inspection, owner/machine isolation and on-demand real OS counters, with **zero prompts/provider calls**. The same smoke exercised the built Hermes CLI through private cache, outbound connector and authenticated MCP using **synthetic native envelopes**, including shared caller PID/resources, zero/missing usage, privacy, turn end, finalize and late callbacks—not live provider callbacks. An earlier actual Darwin collector sample on Node 26.4.0 returned CPU 8.1783%, RSS 150831104 bytes over 244.55 ms; this proves that OS collector, **not** full Mac native-hook, physical cross-machine or TLS deployment acceptance.

Additional disposable proofs passed:

- Official SDK stdio client discovered the two read-only tools and inspected real OS resources through the CLI bridge. All five native installer targets generated private artifacts, refused existing destinations and omitted raw bearers; none was activated.
- The current production image passed hardened smoke as UID/GID 1000 with every capability dropped, no-new-privileges, a read-only root and `/tmp` tmpfs, including the built TypeScript Hermes cache/connector/MCP path with synthetic envelopes. Its baked synthetic source-SHA fixture matched the expected value; an earlier image proof rejected a different expected revision instead of overriding image identity. Native Pi/OMP executables are intentionally absent from the image; their actual CLI smoke ran separately on the host.
- Actual OMP 18.11.3 TUI confirmed native keyword registration/ranges and rendered the current input with a real read-only fleet snapshot. A disposable input hook handled submission before model generation.
- Helm lint/render/package and 15 missing/invalid-input cases passed, including private workloads, no legacy mounts and protected retained PVC declarations.
- Installed Mac Hermes `0.22.1+233.gacc3cad` ran all four configured commands through its actual `hooks doctor` dispatcher using a disposable portable build of the TypeScript CLI and synthetic native payloads. All returned empty stdout and exit 0; observed metadata verified shared dispatcher PID/birth, unknown start/API state, per-turn idle, unchanged usage age and finalized state. A configuration path containing spaces and an apostrophe worked. This does not prove live provider callbacks or physical cross-machine TLS.

These are local disposable proofs, not live installation or deployment. Physical Mac-to-homelab TLS acceptance and full live native-hook usage remain unexercised. No global native settings were changed, live services stopped, historical state imported, commit made, or release published. Historical execution-era test counts and deployment observations are not inspector acceptance.
