# Coordinator

Read-only inspection of existing Pi, OMP, Claude Code, Codex CLI and Hermes sessions on Linux/macOS. Presence, native counters and on-demand CPU/RSS only: no conversations, prompt forwarding, task execution or agent control.

## Deployment and rollback

There are two parts: **one inspector service**, plus **native integrations on each host**. Configuration/Helm values describe desired settings; activation and service startup are explicit commands. Building and artifact generation do not deploy, activate or hot-reload anything. CI/release workflows produce artifacts, not host installations.

Build a stable, reviewed package checkout using Node >=22.19 and pnpm 11.9.0:

```sh
pnpm install --frozen-lockfile
pnpm build
export PACKAGE_ROOT="$PWD"
```

The [esbuild build](scripts/build.ts) keeps Pi dependencies external and bundles OMP dependencies to avoid compatibility-loader traversal. Native host APIs stay external; OMP uses [Bun's native CommonJS support](https://bun.com/docs/runtime/module-resolution#module-systems), not an injected shim. Rebuild and restart OMP after source updates.

For Kubernetes, follow the [chart operator guide](deploy/helm/coordinator/README.md): provide an existing Secret, verified image digest and matching source SHA. The chart creates an inspector Deployment/internal Service, not native plugins, TLS routing, a database or runners. Operators own rollout and supervision. Existing broker state requires the guide's preservation/migration procedure; the plugin does not migrate or delete it.

## Configuration

Provision a separate bearer for each owner/machine principal. The server stores its SHA-256 hash; the host stores the raw token privately. Same-owner reads are allowed; publishing is machine-bound. There is no separate read-only token role.

Generate a token in a **new** private directory; this prints only its enrollment hash:

```sh
export PRIVATE="$HOME/.config/coordinator-inspector-private"
(
  set -eu
  umask 077
  mkdir -m 700 "$PRIVATE"
  mkdir -m 700 "$PRIVATE/state"
  node --input-type=module <<'JS'
import { randomBytes, createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
const token = randomBytes(32).toString('hex');
writeFileSync(`${process.env.PRIVATE}/machine.token`, token, { mode: 0o600, flag: 'wx' });
console.log(createHash('sha256').update(token).digest('hex'));
JS
)
```

Create these **0600** files in that **0700** directory. Replace the hash and OS-local example paths; JSON does not expand `$PRIVATE` or `~`.

`server.json`—inspector configuration:

```json
{
  "host": "127.0.0.1", "port": 8787,
  "principals": [{ "id": "workstation", "owner": "operator", "machineId": "workstation", "tokenHash": "REPLACE_WITH_TOKEN_SHA256" }]
}
```

`inspector.json`—native host/reader configuration:

```json
{
  "inspectorUrl": "http://127.0.0.1:8787",
  "tokenFile": "/home/operator/.config/coordinator-inspector-private/machine.token",
  "machineId": "workstation",
  "stateDirectory": "/home/operator/.config/coordinator-inspector-private/state"
}
```

Files and their immediate parent must be owner-owned/private, with canonical absolute paths, no symlinks and at most 64 KiB per file. Unknown configuration fields are rejected. HTTPS with ordinary certificate verification is required except on loopback. Never put raw credentials in Git, argv, native MCP configuration or logs.

Start a local inspector in the foreground:

```sh
COORDINATOR_CONFIG="$PRIVATE/server.json" node dist/src/main.js inspector
```

`COORDINATOR_CONFIG` selects server configuration. `COORDINATOR_CONNECTOR_CONFIG` selects host configuration; the default is `$XDG_CONFIG_HOME/coordinator/inspector.json` or `~/.config/coordinator/inspector.json`. A missing default disables extension observation; an invalid explicit config is reported. CLI reader/connector modes require configuration.

## Native host setup

Run as the nonroot native user. Choose `omp`, `pi`, `claude`, `codex` or `hermes` and a new destination:

```sh
TARGET=omp
pnpm install:host --target "$TARGET" --destination "$PRIVATE/$TARGET-host" --config "$PRIVATE/inspector.json" --dry-run
pnpm install:host --target "$TARGET" --destination "$PRIVATE/$TARGET-host" --config "$PRIVATE/inspector.json"
export COORDINATOR_CONNECTOR_CONFIG="$PRIVATE/inspector.json"
```

The installer validates prerequisites and creates launchers/config fragments. `--dry-run` writes nothing. It does **not** activate integrations, trust hooks, overwrite native settings or start services. Follow its printed instructions:

| Target | Activation | Presence publisher |
| --- | --- | --- |
| OMP | `omp plugin link "$PACKAGE_ROOT"`; merge only the generated `mcp.json` coordinator entry into native MCP settings. | In-session extension. |
| Pi | `pi install "$PACKAGE_ROOT"`; directly registers `coordinator_session_list` / `coordinator_session_inspect`, without an MCP bridge. | In-session extension. |
| Claude Code | `claude --plugin-dir "$PRIVATE/claude-host"`; approve generated hooks/MCP tools; preserve existing status-line configuration. | Separate `connector.sh`. |
| Codex CLI | Merge generated `hooks/hooks.json` and coordinator `mcp.toml`; trust only Coordinator's handlers in native `/hooks`. | Separate `connector.sh`. |
| Hermes | Merge generated hook entries and `mcp_servers.coordinator` from `mcp.yaml` into the selected profile; no Python plugin installation. | Separate `connector.sh`. |

Generated directories contain configuration, not a copy of the package. Pi/OMP load `dist/plugins/{pi,omp}.js`; MCP/connector launchers reference the build's absolute paths. Keep that package available. Updates require a reviewed build/relink, deliberate native reload/new session, and restart of affected reader bridges/external connectors—not merely editing configuration.

`mcp.sh` is the stdio reader bridge; `connector.sh` publishes external hook metadata. The latter's equivalent foreground command is:

```sh
COORDINATOR_CONNECTOR_CONFIG="$PRIVATE/inspector.json" node dist/src/main.js connector
```

## Plugin publication

The current installer produces per-machine configuration, not portable marketplace packages. The [release workflow](.github/workflows/release.yaml) publishes server/chart/GitHub artifacts, not native marketplace listings.

| Host | Publication route |
| --- | --- |
| [OMP](https://github.com/can1357/oh-my-pi/blob/main/docs/marketplace.md) | Publish a Git-backed `.omp-plugin/marketplace.json` catalog and runnable plugin tree with `package.json` `omp.extensions`. Claude-compatible `.claude-plugin/marketplace.json` is a fallback. npm catalog sources are not supported; ordinary `omp plugin install PACKAGE` is a separate npm route. |
| [Pi](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/packages.md) | Publish an npm package with `keywords: ["pi-package"]` and `pi.extensions`; the keyword makes it eligible for the Pi gallery. Users install with `pi install npm:PACKAGE@VERSION`. Host SDK packages remain host-provided, not bundled. |
| [Hermes](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins/catalog-submission) | Publish a public plugin repository with `plugin.yaml` and a supported entrypoint; submit `plugin-catalog/NAME.yaml` to NousResearch/hermes-agent, pinned to a full reviewed commit SHA. Each update needs another pin-bump PR. |
| [Codex](https://developers.openai.com/plugins/build/plugins) | Publish a portable plugin tree and `.agents/plugins/marketplace.json` in a Git repository. Users add it with `codex plugin marketplace add OWNER/REPO --ref TAG`; plugin installation/enabling and hook trust are separate steps. Current packaging docs explicitly guarantee hook bundles for manual desktop installs; CLI plugin-hook compatibility needs release-specific verification. |
| [Claude Code](https://code.claude.com/docs/en/plugins/publish) | Publish a plugin and `.claude-plugin/marketplace.json` in Git. Users run `claude plugin marketplace add OWNER/REPO`, then `claude plugin install NAME@MARKETPLACE`. Anthropic's directory portal is a separate reviewed route, not submission to `claude-plugins-official`. |

Release gaps and constraints:

- `package.json` is `private: true` and lacks gallery keywords/publication metadata and a `files` allowlist. `npm pack --dry-run --json --ignore-scripts` currently includes the built runtime plus source, tests, CI and Helm files. Prepare an intentional public payload, complete redistribution review, and version package/manifests coherently; the generated Claude manifest currently hardcodes `1.0.0`.
- Keep one runtime implementation with host-specific manifests/catalog entries. Replace author-machine paths with supported plugin-root references or an explicitly provisioned CLI, and ship/provision its dependencies. Do not assume marketplace installation runs build/postinstall scripts, starts a connector or grants hook trust.
- [OpenAI public-directory submissions reject lifecycle hooks](https://developers.openai.com/plugins/build/plugins#bundled-mcp-servers-and-lifecycle-hooks); the full observer therefore cannot be submitted unchanged. The [public MCP portal](https://developers.openai.com/plugins/deploy/submission#authentication-limitations) also lacks the current fixed-bearer authentication flow. An MCP-only offering would be a different scope.
- [Hermes catalog rules](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins/catalog-submission#admission-rules) require secrets through Hermes, not direct credential-file reads; the current token-file design needs an approved credential path before catalog submission. Portable Hermes MCP/skill packages do not automatically activate our shell hooks or external connector. A Skills Hub listing distributes instructions, not this runtime integration.
- Users still need the shared inspector HTTPS service, private machine configuration/credentials, explicit native approvals and, for hook hosts, one separately supervised connector. Validate real cached/package installs and minimum host versions before advertising marketplace compatibility.

## Internal contract

- **Read API:** `/mcp` exposes only `session.list` and `session.inspect`. List requires `limit` 1–100, with cursor/machine/harness filters. Inspect takes the complete returned `SessionRef`: `machineId`, `harness`, `nativeSessionId`, optional `nativeAgentId`.
- **Publishing:** native hosts initiate outbound `/connector/announce`, `/connector/poll`, `/connector/result` and `/connector/disconnect` requests. Detail queries are correlated transient read RPCs, not execution jobs.
- **State/lifetime:** the owner-isolated registry is in memory; restart loses discovery until reannouncement, never native sessions. A Pi/OMP plugin owns one Effect managed runtime; presence/poll failures cancel their peer and shutdown disposes its fibers without killing the host. Failed startup/terminated connectors can restart on a later native event.
- **OMP shortcut:** a leading `coordinate` enriches the current input with fleet presence and an inspection cue. It does not contact other sessions with prompts.

| Timing/bound | Value |
| --- | --- |
| Presence / healthy poll delay | 10 s / 1 s after a completed cycle |
| Receiver liveness / discovery expiry | 30 s / 120 s |
| Native detail / inspector query deadline | 8 s / 10 s |
| HTTP / advisory disconnect deadline | 10 s / 500 ms |
| Connector retry | Exponential from 2 s; 80–120% jitter; hard 30 s cap |
| Notifications | 5 s grace checked on failed retries; one warning across flaps; recovery/reset after 10 healthy seconds |
| Payloads | 64 KiB HTTP/native input; 48 KiB inspection budget |

Connector timeouts and HTTP 409/429/5xx retry and reannounce. HTTP 401/403 retry only after finding a changed token. Other failures end the attempt. Stable recovery resets backoff and warning state; one successful poll does not. Terminal configuration/authentication failures and defects are reported without hiding cancellation. Diagnostics use native UI or stderr and expose safe operation/code/status, not credentials or rejected payloads. `/livez` is public; `/readyz` and `/healthz` require an enrolled bearer. Request deadlines do not bound arbitrary uninterruptible work.

## Metrics and provenance

Only exposed native metadata/getters are observed; no transcript/screen scraping or retained prompt/tool contents. Pi/OMP use context/usage updates. Claude optionally adds status-line counters/native PID roster; Codex token counters require an already-attached app-server's v2 notifications. The `claude-statusline` / `codex-notification` stdin adapters do not attach to or launch agents. Hermes uses start, post-API, turn-end and finalize hooks: end means idle, finalize removes the record, and late callbacks cannot recreate it.

CPU/RSS sampling runs only on inspection, over approximately 200 ms. PID plus OS birth identity protects attribution. OMP/Hermes host processes are shared, not exclusive session usage; CPU 100% means one core and RSS is not unique physical memory. Missing/unsupported fields are `unavailable`, never fabricated zeroes. Latest updates and aggregates stay separate, with cached ages and provenance flags retained. Exact fields/units live in [inspection.ts](src/inspection.ts), rather than a duplicate metric catalog here.

## Source

| File | Responsibility |
| --- | --- |
| [inspection.ts](src/inspection.ts) | Strict config/wire schemas, identity, metric provenance and tool descriptors. |
| [inspector.ts](src/inspector.ts) | `Inspector` service: authentication, registry, liveness and query correlation. |
| [mcp.ts](src/mcp.ts) | `Mcp` service: bounded HTTP/MCP transport and stdio reader. |
| [native.ts](src/native.ts) | Private files/cache, native projections and connector policy. |
| [plugin.ts](src/plugin.ts) | Pi/OMP hooks, observers, tools and runtime ownership. |
| [resources.ts](src/resources.ts) | Linux/macOS process identity and on-demand sampling. |
| [main.ts](src/main.ts) | CLI entrypoints and layer composition. |

## Verification

Typechecking uses Effect tsgo. All test suites import `@effect/vitest`; native resource sampling uses `it.live`, while deterministic parsing uses `it.effect`.

```sh
pnpm check
pnpm test
pnpm smoke
```

Smoke uses disposable native sessions without model prompts; it is not deployment/live-provider acceptance. `COORDINATOR_TEST_PI` / `COORDINATOR_TEST_OMP` select existing absolute CLI paths. Source-package OMP needs Bun >=1.3.14; compiled OMP has its own runtime. A loading/discovery failure is a failed smoke, not deployment proof.

After activation, use `node dist/src/main.js list`, then pass the selected `{"ref": ...}` on stdin to `node dist/src/main.js inspect`; check the intended session and metrics. CLI/installer `--help` list supported options. Versions are pinned in [package.json](package.json); [third-party notices](THIRD_PARTY_NOTICES.md) apply to redistribution.
