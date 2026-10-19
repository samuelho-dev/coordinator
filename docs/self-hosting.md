# Private inspector operator guide

## Scope and release status

Coordinator now inspects native Pi, OMP, Hermes, Codex CLI and Claude Code sessions. It does not start agents, execute tasks, manage terminals, replace Hermes workflows, or provide a broker/runner/database/controller. Only `session.list` and `session.inspect` are model-facing tools, both read-only. See the [complete capability catalog](../README.md) and [implemented source map](src-consolidation-plan.md); the [historical orchestration research](agent-orchestration-research.md) is not an installation guide.

The implementation has passed local build/type/tests (6 suites, 52 tests), actual disposable Linux Pi 1.0.4 and compiled OMP 18.11.3 CLI-extension/HTTP/connector/resource smoke with no prompts or provider calls, and the built Hermes CLI/cache/connector/MCP smoke with synthetic native envelopes, also in the hardened image. Earlier proofs include a real Darwin OS sampler, native OMP TUI, stdio and chart checks. These do not prove live Hermes provider callbacks, physical cross-machine/private TLS acceptance or deployment. Check the [current verification record](../README.md) for exact evidence and remaining boundaries. No published signed release, live deployment, global host configuration change or state migration is implied by this guide.

## 1. Preserve existing installations first

This is a breaking product/protocol cutover, not an in-place task-store upgrade. Preserve **all real databases, journals, backups, PVCs, old images and rollback coordinates**. `prisma/` and `migrations/` remain historical rollback source assets, not runtime prerequisites. The new CLI cannot recover/import old tasks. Retired broker, runner, SDK-session, backup, database and remote-install commands are not supported aliases.

An existing installation needs a separate approved rollout: inventory the actual live desired state and all tracked storage, drain old clients/work using the old release's procedures, fence old owners, preserve rollback data, then explicitly migrate downstream routes from `<release>-broker` to `<release>-inspector` and reload selected clients. Rendering a new chart, `Recreate`, and PVC retention annotations do not perform drain or owner fencing. Do not synchronize a fresh overlay over a stateful old release or let GitOps prune storage. No systemd service, SSH bootstrap or remote installation is provided here.

## 2. Acquire an exact private source/release

Use an authorized exact-source checkout and its lockfile:

```sh
pnpm install --frozen-lockfile
pnpm build
```

The package declares Node `>=22.19` and pnpm `11.9.0`; the production image uses Node `24.21.0`. The CLI is `node dist/src/main.js` (installed package bin: `coordinator`). Bundles are `dist/plugins/pi.js` and `dist/plugins/omp.js`; native SDKs are host/development dependencies, not inspector runtime dependencies.

The npm/source OMP SDK requires **Bun >=1.3.14**; do not load it as a Node SDK. A separately compiled OMP executable has its own runtime/version boundary. The package pins OMP SDK 18.6.1, while the recorded real compiled-host smoke used 18.11.3: these are different artifacts, not a blanket compatibility promise. Check `pi --version`, `omp --version`, and the selected native API before activation on older binaries. Pi 1.0.4's installed `docs/mcp.md` verifies user `~/.pi/agent/mcp.json` and project `.pi/mcp.json` paths; project trust and native reload still apply ([upstream MCP documentation](https://github.com/earendil-works/pi-mono/blob/main/packages/coding-agent/docs/mcp.md)).

## 3. Private inspector and connector configuration

[The source schemas](../src/inspection.ts) and [private-file reader](../src/native.ts) are authoritative. Inspector configuration and connector configuration are **different JSON shapes**, decoded with unknown properties rejected. No old `plugin.json`, database URL, repository enrollment, execution roles or model credentials belong in either.

Use separate bearer tokens per owner/machine principal. A principal contains `id`, `owner`, `machineId`, and lowercase SHA-256 `tokenHash`. Same-owner principals can read each other's observations; publishing is restricted to the principal's machine. There is no read-only bearer role: protect every enrolled token accordingly. Hash the exact token bytes consumed after trimming. Connector token files accept 16–4096 non-whitespace characters; cryptographically random 32-byte hex tokens are a suitable choice.

Run as the actual nonroot host owner. Select a **new**, canonical absolute private directory, not a symlink or path containing `..`. Config/token files must be owner-owned regular files, with no group/other permissions (use **0600**), in an owner-owned canonical immediate parent with no group/other permissions (use **0700**). Reads reject final symlinks and files over 64 KiB. Protect the state directory in the same way; it stores bounded metadata observations, not conversations or task journals. Do not reuse old state directories.

For example, create a new owner directory, then generate a token and a nonsecret hash file without printing either or putting a bearer in argv/environment:

```sh
umask 077
# Replace this path with your own canonical absolute NEW directory.
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
```

Keep tracing off. Provision raw tokens through private files/your secret backend only, never shell arguments, logs, plaintext Git or native MCP settings. Copy only the hash into the inspector's enrollment through your protected editor/secret tooling. The following examples contain no real credentials; replace `REPLACE_WITH_64_LOWERCASE_HEX_SHA256` with the generated hash before use.

**Inspector server file**, e.g. `/home/operator/.config/coordinator-inspector-private/server.json`:

```json
{
  "host": "127.0.0.1",
  "port": 8787,
  "principals": [
    {
      "id": "workstation-reader",
      "owner": "example-owner",
      "machineId": "example-workstation",
      "tokenHash": "REPLACE_WITH_64_LOWERCASE_HEX_SHA256"
    }
  ]
}
```

**Connector/client file**, e.g. `/home/operator/.config/coordinator-inspector-private/inspector.json`:

```json
{
  "inspectorUrl": "http://127.0.0.1:8787",
  "tokenFile": "/home/operator/.config/coordinator-inspector-private/machine.token",
  "machineId": "example-workstation",
  "stateDirectory": "/home/operator/.config/coordinator-inspector-private/state"
}
```

For another machine, create a distinct token/principal with the same owner and its own machine ID; use real private `https://inspector.example.net` DNS and an OS-local private token/config/state path. Never copy Linux absolute paths into Mac configuration unchanged. TLS is required except loopback (`localhost`, `127.0.0.1`, `[::1]`); URLs cannot contain credentials, query or fragment. Preserve ordinary certificate-chain/hostname verification and system DNS; no custom TCP relay or verification bypass. Supply a trusted public CA bundle through `NODE_EXTRA_CA_CERTS` only when necessary, then restart the affected Node process after trust rotation. Never distribute a CA private key.

## 4. Run the plain inspector and readers

These are foreground commands; choose explicitly which processes to start. There is no controller or service installer.

```sh
COORDINATOR_CONFIG="$PRIVATE/server.json" node dist/src/main.js inspector
```

In separate shells with the selected private connector config:

```sh
export COORDINATOR_CONNECTOR_CONFIG="$PRIVATE/inspector.json"
node dist/src/main.js help
node dist/src/main.js list
node dist/src/main.js inspect <<'JSON'
{"ref":{"machineId":"example-workstation","harness":"pi","nativeSessionId":"COPY_EXACT_ID_FROM_LIST"}}
JSON
node dist/src/main.js mcp
```

`inspect` accepts bounded JSON on stdin, not a session-ID flag. Copy the complete `ref` from `list`, including `nativeAgentId` when present. `list` requests up to 100 entries; native MCP `session.list` supports its schema's pagination/filter inputs. `mcp` is the stdio bridge intended for a native MCP launcher, not an interactive prompt. It loads credentials privately and exposes only the two read-only tools. Ordinary command errors exit 1; metadata hook/statusline/notification adapters report redacted errors and exit 0 so inspection failures do not break native work. Hook commands produce no display output.

The connector config defaults to `$XDG_CONFIG_HOME/coordinator/inspector.json`, or `~/.config/coordinator/inspector.json`. Missing default config disables native extension enrollment; an explicit missing/invalid/insecure file fails. Never point it at the server JSON. The external cache publisher is started explicitly:

```sh
COORDINATOR_CONNECTOR_CONFIG="$PRIVATE/inspector.json" node dist/src/main.js connector
```

Pi/OMP extensions connect directly in their actual session; Claude/Codex/Hermes hooks need that external connector to publish their private metadata cache. The connector uses outbound authenticated HTTP announce/poll/result/disconnect RPCs, not inbound Mac listeners or durable jobs. Basic presence is periodic (10 seconds); receiver liveness is 30 seconds, expiry 120 seconds, detail request timeout 10 seconds. Restart loses registry discovery until reannouncement, never native work. Liveness is connector receipt, not proof that a cached Codex process still lives. Detailed native getters and CPU/RSS sampling run only on demand. Unavailable values are not zero; cached native update timestamps do not refresh on read. Shared/unknown PIDs are not charged to exclusive aggregates; RSS sums are not unique physical RAM and 100% CPU means one core.

## 5. Generate artifacts, then activate each native harness manually

The installer only creates a **NEW** directory beneath an existing private owner directory. It validates config/token access and refuses root/existing destinations. It does not overwrite settings, statusLine, profiles or credentials; it does not install remotely, reload a TUI or start a connector. Use a separate new destination for each target. `--runtime /absolute/path/to/node` optionally pins the generated launcher; otherwise it uses the installer's Node executable. Do not move/delete the exact-source build afterward: generated artifacts refer to its absolute paths.

```sh
pnpm install:host --target pi --destination "$PRIVATE/pi-host" --config "$PRIVATE/inspector.json" --dry-run
pnpm install:host --target pi --destination "$PRIVATE/pi-host" --config "$PRIVATE/inspector.json"
pnpm install:host --target omp --destination "$PRIVATE/omp-host" --config "$PRIVATE/inspector.json"
pnpm install:host --target claude --destination "$PRIVATE/claude-host" --config "$PRIVATE/inspector.json"
pnpm install:host --target codex --destination "$PRIVATE/codex-host" --config "$PRIVATE/inspector.json"
```

Every target includes `connector.sh` and `mcp.sh`. Review the installer's printed instructions and explicitly approve only the intended changes:

| Harness | Generated artifact and manual activation |
| --- | --- |
| Pi | `package.json` points at `dist/plugins/pi.js`; run `pi install "$PRIVATE/pi-host"`. Merge **only** `mcpServers.coordinator` from generated `mcp.json` into selected `~/.pi/agent/mcp.json` or project `.pi/mcp.json`, preserving all entries. Launch with `COORDINATOR_CONNECTOR_CONFIG` and reload as required by native Pi. |
| OMP | `package.json` points at `dist/plugins/omp.js`; run `omp plugin link "$PRIVATE/omp-host"`. Merge only the generated coordinator MCP entry into selected `.omp/mcp.json`, preserving other entries; launch with the explicit connector-config environment. |
| Claude Code | `.claude-plugin/plugin.json`, `hooks/hooks.json`, `.mcp.json`; launch `claude --plugin-dir "$PRIVATE/claude-host"` and approve the read-only MCP tools/metadata hooks. Start its generated `connector.sh` separately as the same owner. Existing custom statusLine stays untouched. |
| Codex CLI | `hooks/hooks.json` and `mcp.toml`; manually merge generated hooks into selected `~/.codex/hooks.json` or project `.codex/hooks.json`, and only `[mcp_servers.coordinator]` plus its env table into selected `config.toml`. Preserve existing hooks/tables and approve native hooks. Start its generated `connector.sh` separately. |
| Hermes | `hooks.yaml` (JSON-compatible YAML), `mcp.yaml` and equivalent `mcp.json`; manually append only generated observer entries to `hooks:` in the selected profile's `config.yaml`, and merge only `mcp_servers.coordinator` from `mcp.yaml` into that same file. Preserve existing hooks, MCP servers and all other settings. Review/approve only the intended native commands, never autoaccept all. Start generated `connector.sh` separately to publish metadata. |

For Hermes choose a **new** private artifact directory, not a plugin/profile destination:

```sh
pnpm install:host --target hermes --destination "$PRIVATE/hermes-host" --config "$PRIVATE/inspector.json"
```

These are configuration includes and launchers for the shared TypeScript CLI, not a separate Hermes product or Python plugin. No `HERMES_HOME/plugins` installation, plugin enable command or profile overwrite is needed. Reader-only Hermes can merge just the MCP entry and omit observation hooks/connector; publishing its native sessions requires the hooks plus connector. Hermes itself remains upstream's native runtime; Coordinator adds no Python dependency.

The [native shell-hook contract](https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks#shell-hooks) and [inspected upstream revision `acc3cada0da8`](https://github.com/NousResearch/hermes-agent/blob/acc3cada0da8/agent/shell_hooks.py) execute parsed argv directly (`shell=False`), not through a shell. Generated hook commands are `/usr/bin/env COORDINATOR_CONNECTOR_CONFIG='<config>' '<node>' '<main>' hook hermes`: `env` replaces itself with Node, preserving the actual dispatcher parent PID. Native command consent remains required; support is not asserted for every stable release.

Only `on_session_start` establishes a metadata record; `post_api_request` updates latest successful request usage, not account spend or session-lifetime totals. `on_session_end` means idle/turn end, not session removal. `on_session_finalize` closes the old session (preferring `extra.old_session_id`); late callbacks cannot resurrect it. The actual dispatcher PID plus OS birth identity is a **shared hook host**, not an exclusive session process, wire PID or guessed TUI ancestor. While that exact host lives, cache reads refresh presence only, preserving native activity/usage ages; dead/reused hosts are absent. Without verified host identity, metadata is bounded to 120 seconds with no process/resources. No heartbeat daemon or extra Hermes/model session is started; unknown/legacy files remain untouched.

Claude's connector can use native `claude agents --json` for PID roster identity ([native agents documentation](https://code.claude.com/docs/en/cli-reference)); ordinary hooks provide metadata. For optional metrics, manually compose your existing [status-line command](https://code.claude.com/docs/en/statusline) to pass the **same stdin JSON** to `node /absolute/coordinator/dist/src/main.js claude-statusline` with the connector-config environment, while preserving your existing output. Do not replace the statusLine configuration. This adapter itself displays nothing. Current-context/latest-response tokens are not lifetime totals; context input/cache basis excludes output, and cost is the native client estimate.

Codex ordinary hooks provide metadata and do not fabricate PID/token counters. Only an **already attached** native app-server source can feed bounded stdin to `node dist/src/main.js codex-notification`: its envelope is `version: 2`, `source: "attached-app-server"`, `method: "thread/tokenUsage/updated"`, with native thread/turn/tokenUsage params ([native app-server contract](https://developers.openai.com/codex/app-server/)). Do not launch a hidden app-server, new thread or SDK session to pretend to observe arbitrary TUIs. Without that source, those counters remain unavailable.

OMP's leading `coordinate` keyword transforms only the current input with an actual read-only session snapshot/cue; it grants no role, lease, forwarding or execution capability. Native enabled core keyword glow/custom editors are preserved, but the replace-only native API cannot enumerate arbitrary third-party registrations: keyword composability is not guaranteed. Other input is unchanged.

## 6. Kubernetes, TLS and release approval

Use the [chart operator reference](../deploy/helm/coordinator/README.md) for required values, existing Secret keys, private staging, exact probes and legacy PVC retention. The chart creates only one inspector Deployment, one internal ClusterIP Service and explicitly retained legacy claims. It supplies no database, migrations, agent SDK/runtime, controller, ingress, DNS, NetworkPolicy or secret manager. TLS termination and policy are operator-owned: allow required DNS, probe/gateway and outbound native-client traffic through the private route, without exposing enrollment tokens.

Image identity has two independent sides: `COORDINATOR_IMAGE_SHA` is baked by the trusted image build and returned in authenticated `x-coordinator-source-sha`; chart `sourceSha` sets **only** `COORDINATOR_EXPECTED_SOURCE_SHA`, which readiness compares against that header. Never override the baked image SHA in a Pod or smoke command to make it agree. A matching header or valid hash syntax alone is not provenance.

Before promotion require the exact immutable main source's normal CI, hardened exact-image smoke, verified image digest/signature/expected OIDC producer, SBOM and SLSA source/repository/commit attestation, verified chart artifact, and a reviewed downstream GitOps PR pinning digest plus source SHA. Do not borrow an old green run or publish/sign as a different producer. Source/image/chart remain private until a separate visibility/legal owner decision; inspect [notices](../THIRD_PARTY_NOTICES.md) before redistribution. Neither local smoke nor GitOps sync proves real private TLS/MCP/native-host acceptance. No command in this guide authorizes deployment, stop, migration or pruning of an existing installation.
