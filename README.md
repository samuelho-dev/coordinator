# Coordinator

Read-only inspection of existing Pi, OMP, Claude Code, Codex CLI and Hermes sessions on Linux/macOS. Presence, native counters and on-demand CPU/RSS only: no conversations, prompt forwarding, task execution or agent control.

## Deployment and rollback

There are two parts: **one inspector service**, plus **native integrations on each host**. Configuration/Helm values describe desired settings; activation and service startup are explicit commands. Building and artifact generation do not deploy, activate or hot-reload anything. CI/release workflows produce artifacts, not host installations.

The API and standalone CLI use Node >=22.19; no additional runtime is required. Build a stable, reviewed package checkout using pnpm 11.9.0:

```sh
pnpm install --frozen-lockfile
pnpm build
export PACKAGE_ROOT="$PWD"
```

The [esbuild build](scripts/build.ts) keeps Pi dependencies external and bundles OMP dependencies to avoid compatibility-loader traversal. Native host APIs stay external; OMP uses [Bun's native CommonJS support](https://bun.com/docs/runtime/module-resolution#module-systems), not an injected shim. Rebuild and restart OMP after source updates.

For Kubernetes, follow the [chart operator guide](deploy/helm/coordinator/README.md): provide an existing Secret, verified image digest and matching source SHA. The chart creates an inspector Deployment/internal Service, not native plugins, TLS routing, a database or runners. Operators own rollout and supervision. Existing broker state requires the guide's preservation/migration procedure; the plugin does not migrate or delete it.

## Configuration

### Operator/API setup

Provision owner-scoped bearer keys. The server stores SHA-256 hashes, not raw tokens; every principal contains only `id`, `owner` and `tokenHash`. A key authorizes reads and publishing across that owner's fleet, not one physical device. Same-owner keys can announce multiple installations; installation IDs are labels, not device authentication. Anyone holding an owner key can publish under any of that owner's machine labels. Revoking a shared key revokes every holder; there is no separate read-only or health-token role.

Generate a key in a **new** private directory from the installed package checkout; this prints only its hash:

```sh
export PRIVATE="$HOME/.config/coordinator-inspector-private"
(
  set -eu
  cd "$PACKAGE_ROOT"
  node --input-type=module <<'JS'
import { Config, Console, Crypto, Effect, FileSystem } from 'effect';
import { Hex } from 'effect/encoding';
import { NodeRuntime, NodeServices } from '@effect/platform-node';
NodeRuntime.runMain(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const crypto = yield* Crypto.Crypto;
  const directory = yield* Config.String('PRIVATE');
  yield* fs.makeDirectory(directory, { mode: 0o700 });
  yield* fs.chmod(directory, 0o700);
  const token = Hex.encode(yield* crypto.randomBytes(32));
  const file = `${directory}/owner.token`;
  yield* fs.writeFileString(file, token, { mode: 0o600, flag: 'wx' });
  yield* fs.chmod(file, 0o600);
  yield* Console.log(Hex.encode(yield* crypto.digest('SHA-256', new TextEncoder().encode(token))));
}).pipe(Effect.provide(NodeServices.layer)));
JS
)
```

Create `server.json` as **0600** in that **0700** directory, substituting the hash:

```json
{
  "host": "127.0.0.1", "port": 8787,
  "principals": [{ "id": "operator-key", "owner": "operator", "tokenHash": "REPLACE_WITH_TOKEN_SHA256" }]
}
```

Start a local inspector in the foreground:

```sh
COORDINATOR_CONFIG="$PRIVATE/server.json" node dist/src/main.js inspector
```

`COORDINATOR_CONFIG` selects server configuration, not client credentials. Authentication is mandatory: no anonymous fallback. HTTPS with ordinary certificate verification is required except on loopback.

### Native-owner/client setup

Clients need only `inspectorUrl` and `apiToken`, supplied through private JSON or `COORDINATOR_API_URL` / `COORDINATOR_API_TOKEN`. The environment overrides corresponding file fields. Have the native owner's secret manager inject the token into the environment of the native host, reader bridge and separately supervised connector; never put raw keys in Git, argv, generated native MCP configuration or logs:

```sh
export COORDINATOR_API_URL="https://inspector.example.net"
# Provision COORDINATOR_API_TOKEN through the owner's secret manager before starting clients.
node dist/src/main.js list
```

Alternatively, use a private client file (`inspector.json`), with either `apiToken` or the optional `tokenFile`:

```json
{
  "inspectorUrl": "https://inspector.example.net",
  "tokenFile": "/home/operator/.config/coordinator-inspector-private/owner.token"
}
```

Resolved `apiToken` takes precedence over `tokenFile`. Explicit `--config` (where supported) selects the client file before `COORDINATOR_CONNECTOR_CONFIG`, then `$XDG_CONFIG_HOME/coordinator/inspector.json` or `~/.config/coordinator/inspector.json`. Private files and their immediate parent must be owner-owned/private (0600/0700), canonical absolute paths without symlinks; files are limited to 64 KiB. JSON does not expand `~` or environment variables. Unknown fields are rejected. An explicit missing/invalid file is an error; a missing default is allowed for env-only setup. No configured endpoint leaves extension observation inactive, not pointed implicitly at localhost. Network consumers require credentials and never fall back after authentication rejection.

Native capture automatically creates one private persistent UUID in `machine-id` beneath `$XDG_STATE_HOME/coordinator`, otherwise `~/.local/state/coordinator` on Linux or `~/Library/Application Support/coordinator` on macOS. The state root is canonical, owner-owned and 0700; `machine-id` is 0600. All harnesses/processes on that installation share it across endpoints, key rotations and profiles. Different installations get different IDs; unsafe/corrupt identity files fail rather than being replaced. Do not configure `machineId` or `stateDirectory`. Read-only list/inspect/MCP clients and artifact generation do not create local identity/state.

## Native host setup

Run as the nonroot native user with the endpoint and credentials provisioned as above. Choose `omp`, `pi`, `claude`, `codex` or `hermes` and a new destination:

```sh
TARGET=omp
pnpm install:host --target "$TARGET" --destination "$PRIVATE/$TARGET-host" --dry-run
pnpm install:host --target "$TARGET" --destination "$PRIVATE/$TARGET-host"
```

`--config "$PRIVATE/inspector.json"` is optional. An explicit flag or `COORDINATOR_CONNECTOR_CONFIG` selection is retained as a literal private-file pointer, never the raw token. Without either, clients look up `$XDG_CONFIG_HOME/coordinator/inspector.json` or `~/.config/coordinator/inspector.json` at runtime; the installer does not freeze that file's current endpoint into launchers or hooks. File-backed endpoint/credential changes take effect after ordinary process restart. Keep the same configuration environment available to the native host, hooks and separately supervised connector:

- **Pi/OMP:** launch the harness with the selected private-file pointer or default-file environment, plus any URL/token overrides. Pi runs in-process; the pinned OMP stdio transport merges `Bun.env` before server `env`, so no token interpolation/config entry is needed.
- **Codex:** generated `env_vars` forwards `COORDINATOR_API_TOKEN`, `COORDINATOR_API_URL` and `XDG_CONFIG_HOME` by name. No URL literal masks a runtime override. A selected private-file pointer stays in `env`; [Codex merges literal `env` after forwarded names](https://github.com/openai/codex/blob/main/codex-rs/rmcp-client/src/utils.rs), so an ambient config pointer cannot replace the installer's explicit selection.
- **Claude Code:** generated MCP configuration uses [documented `${VAR}` expansion](https://code.claude.com/docs/en/mcp#environment-variable-expansion-in-mcp-json) only for URL/token overrides present during generation, and for `XDG_CONFIG_HOME` when set. Private-file-only setup does not require URL/token variables. Keep every referenced variable provisioned whenever launching Claude; unset references are not optional and can retain literal text. Remove an override reference deliberately when switching back to file credentials.
- **Hermes:** generated MCP `env` likewise references only URL/token overrides present during generation and `XDG_CONFIG_HOME` when set, never their credential values. Its [stdio environment is filtered](https://github.com/NousResearch/hermes-agent/blob/main/tools/mcp_tool_config.py), so these entries preserve the custom default-file location and explicitly provisioned overrides. Private-file-only setup emits no URL/token references. Use a Hermes version supporting `${VAR}` interpolation and keep referenced variables provisioned in the selected profile's environment/secret scope, not an unrelated gateway profile.

Secret-manager provisioning must reach the native host and separately supervised connector; an installer's environment does not persist into later native launches. No generated MCP entry embeds a bearer in `env` or argv.

The installer validates prerequisites and creates launchers/config fragments. `--dry-run` writes nothing. It does **not** activate integrations, trust hooks, overwrite native settings or start services. Follow its printed instructions:

| Target | Activation | Presence publisher |
| --- | --- | --- |
| OMP | `omp plugin link "$PACKAGE_ROOT"`; merge only the generated `mcp.json` coordinator entry into native MCP settings. | In-session extension. |
| Pi | `pi install "$PACKAGE_ROOT"`; directly registers `coordinator_session_list` / `coordinator_session_inspect`, without an MCP bridge. | In-session extension. |
| Claude Code | `claude --plugin-dir "$PRIVATE/claude-host"`; approve generated hooks/MCP tools; preserve existing status-line configuration. | Separate `connector.sh`. |
| Codex CLI | Merge generated `hooks/hooks.json` and coordinator `mcp.toml`; trust only Coordinator's handlers in native `/hooks`. | Separate `connector.sh`. |
| Hermes | Merge generated hook entries and `mcp_servers.coordinator` from `mcp.yaml` into the selected profile; no Python plugin installation. | Separate `connector.sh`. |

Generated directories contain configuration, not a copy of the package. Pi/OMP load `dist/plugins/{pi,omp}.js`; MCP/connector launchers reference the build's absolute paths. Keep that package available. Updates require a reviewed build/relink, deliberate native reload/new session, and restart of affected reader bridges/external connectors—not merely editing configuration. Environment or config-token changes require process reload; private token files are reread for rotation/recovery. Staged server Secret changes require a controlled inspector restart.

`mcp.sh` is the stdio reader bridge; `connector.sh` publishes external hook metadata. The latter's equivalent foreground command is:

```sh
COORDINATOR_CONNECTOR_CONFIG="$PRIVATE/inspector.json" node dist/src/main.js connector
```

Hook/status-line/notification capture is local and can initialize identity/cache on first use while offline, without a token; it is not an offline delivery queue. Network publishing still needs credentials and a running publisher. Metadata cache is bounded to 1024 observations and 64 KiB per file, normally expires after 120 s, and may retain Hermes observations while the attributed host remains live. No transcripts are collected.

## Coordinated migration

This is a clean schema cutover, with no legacy aliases or compatibility shims. Before a separately approved rollout, coordinate the server and all native owners: provision fresh owner keys privately, replace server principals with `{ id, owner, tokenHash }`, update the chart's `health.token` to match an owner key, and replace client files with endpoint/credential fields only. Remove manual `machineId`/`stateDirectory` settings, regenerate affected installer artifacts, then deliberately restart the inspector and affected native hosts/bridges/connectors. Preserve the automatically created local identity on subsequent upgrades. Old server/client configuration is rejected; do not mix releases expecting the old machine-bound schema. These instructions do not perform a live rollout or rotate live keys.


## Plugin publication

The npm candidate is `@samuelho-dev/coordinator@0.3.0-beta.1`; publication requires the exact-source CI-accepted tarball and owner authorization. The [npm workflow](.github/workflows/npm-release.yaml) promotes that accepted artifact; [first-publication authentication and trust setup](#first-npm-publication) remain operator prerequisites. The existing [release workflow](.github/workflows/release.yaml) publishes server/chart/GitHub artifacts, not native marketplace listings. Generated per-machine configuration is not a portable marketplace package.

Once published, use your native package manager, then follow [configuration](#configuration) and [native host setup](#native-host-setup):

```sh
omp plugin install @samuelho-dev/coordinator@0.3.0-beta.1
pi install npm:@samuelho-dev/coordinator@0.3.0-beta.1
```

| Host | Publication route |
| --- | --- |
| [OMP](https://github.com/can1357/oh-my-pi/blob/main/docs/marketplace.md) | Publish a Git-backed `.omp-plugin/marketplace.json` catalog and runnable plugin tree with `package.json` `omp.extensions`. Claude-compatible `.claude-plugin/marketplace.json` is a fallback. npm catalog sources are not supported; ordinary `omp plugin install PACKAGE` is a separate npm route. |
| [Pi](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/packages.md) | Publish an npm package with `keywords: ["pi-package"]` and `pi.extensions`; the keyword makes it eligible for the Pi gallery. Users install with `pi install npm:PACKAGE@VERSION`. Host SDK packages remain host-provided, not bundled. |
| [Hermes](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins/catalog-submission) | Publish a public plugin repository with `plugin.yaml` and a supported entrypoint; submit `plugin-catalog/NAME.yaml` to NousResearch/hermes-agent, pinned to a full reviewed commit SHA. Each update needs another pin-bump PR. |
| [Codex](https://developers.openai.com/plugins/build/plugins) | Publish a portable plugin tree and `.agents/plugins/marketplace.json` in a Git repository. Users add it with `codex plugin marketplace add OWNER/REPO --ref TAG`; plugin installation/enabling and hook trust are separate steps. Current packaging docs explicitly guarantee hook bundles for manual desktop installs; CLI plugin-hook compatibility needs release-specific verification. |
| [Claude Code](https://code.claude.com/docs/en/plugins/publish) | Publish a plugin and `.claude-plugin/marketplace.json` in Git. Users run `claude plugin marketplace add OWNER/REPO`, then `claude plugin install NAME@MARKETPLACE`. Anthropic's directory portal is a separate reviewed route, not submission to `claude-plugins-official`. |

Release gaps and constraints:

- The manifest now has a runtime-only `files` allowlist, Pi gallery metadata, public beta settings, and `coordinator` / `coordinator-install` binaries. Installer and generated-manifest versions come from `package.json`. Before publication, build and exercise a clean production-only tarball install, reconcile its bundled notices, finish retained-history/confidential-material review, and confirm npm scope/version authorization.
- Keep one runtime implementation with host-specific manifests/catalog entries. Replace author-machine paths with supported plugin-root references or an explicitly provisioned CLI, and ship/provision its dependencies. Do not assume marketplace installation runs build/postinstall scripts, starts a connector or grants hook trust.
- [OpenAI public-directory submissions reject lifecycle hooks](https://developers.openai.com/plugins/build/plugins#bundled-mcp-servers-and-lifecycle-hooks); the full observer therefore cannot be submitted unchanged. The [public MCP portal](https://developers.openai.com/plugins/deploy/submission#authentication-limitations) also lacks the current fixed-bearer authentication flow. An MCP-only offering would be a different scope.
- [Hermes catalog rules](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins/catalog-submission#admission-rules) require secrets through Hermes. Catalog packaging must use approved Hermes secret provisioning, not the optional direct-token-file mode; local environment forwarding alone is not catalog acceptance. Portable Hermes MCP/skill packages do not automatically activate our shell hooks or external connector. A Skills Hub listing distributes instructions, not this runtime integration.
- Users still need the shared inspector HTTPS service, owner credentials supplied privately, explicit native approvals and, for hook hosts, one separately supervised connector. Validate real cached/package installs and minimum host versions before advertising marketplace compatibility.

### First npm publication

- Authenticate with native `npm login` as an account authorized for `@samuelho-dev`; satisfy 2FA and inspect existing package/access/version state. GitHub or AWS access does not grant npm rights. An anonymous 404 is not creation permission; stop if ownership or private-package state is ambiguous.
- Publish only the accepted tarball from successful current-main CI. Apply the [release workflow's](.github/workflows/npm-release.yaml) source/run/attempt, required-job, unique-unexpired-artifact, manifest/repository, normalized source-contract, SHA256 and unused-version checks. Do not rebuild, repack a dirty checkout or create a dummy bootstrap version. Explicitly publish that archive with `--ignore-scripts --access public --tag beta --registry=https://registry.npmjs.org`; an interactive first publication has no hosted npm provenance.
- For later releases, configure [npm's trusted publisher](https://docs.npmjs.com/trusted-publishers/) for GitHub owner `samuelho-dev`, repository `coordinator`, workflow `npm-release.yaml`, environment `npm-public`, with direct publishing allowed. Protect that GitHub environment with maintainers/reviewers and main-only restrictions; naming it in YAML does not create those rules. The hosted OIDC workflow publishes the verified archive with provenance, without an npm token fallback.


## Internal contract

- **Read API:** `/mcp` exposes only `session.list` and `session.inspect`. List requires `limit` 1–100, with cursor/machine/harness filters. Inspect takes the complete returned `SessionRef`: `machineId`, `harness`, `nativeSessionId`, optional `nativeAgentId`.
- **Publishing:** native hosts initiate outbound `/connector/announce`, `/connector/poll`, `/connector/result` and `/connector/disconnect` requests. Detail queries are correlated transient read RPCs, not execution jobs.
- **Startup:** generated integrations do not await the inspector during activation. Pi/OMP publish in the background; Claude/Codex/Hermes hooks capture locally; the local stdio bridge initializes and lists tools without a remote handshake. Explicit inspection/`coordinate` requests may await a bounded remote reply. Local configuration, filesystem and native loading still run during initialization.
- **State/lifetime:** the owner-isolated registry is in memory; restart loses discovery until reannouncement, never native sessions. A Pi/OMP plugin owns one Effect managed runtime; presence/poll failures cancel their peer and shutdown disposes its fibers without killing the host. Closing the stdio reader's stdin ends its scope and cancels pending HTTP reads. Failed startup/terminated connectors can restart on a later native event.
- **Effect composition:** `NodeServices.layer` provides filesystem/path/crypto/process/stdio services; `Host.layer` provides host facts. `Mcp.layer` privately supplies its HTTP client. The CLI scope owns the inspector/server; one Pi/OMP `ManagedRuntime` owns plugin fibers/layers through shutdown. CLI-only stdin closes on completion; plugin stdin remains host-owned. Layer construction, provisioning and sharing follow the [v4 layer contract](https://effect.website/docs/v4/api/effect/Layer).
- **Node bindings:** [`platform.ts`](src/platform.ts) confines native host facts and CLI-owned stdin alongside the server factory required by [`NodeHttpServer.layerServer`](https://effect.website/docs/v4/api/platform-node/NodeHttpServer#layerServer) and constant-time comparison absent from [`Crypto`](https://effect.website/docs/v4/api/effect/Crypto#Crypto-interface). Filesystem checks use `FileSystem`; execute permission uses Effect's scoped process service running `/bin/test -x`, never the selected runtime. Native SDK callbacks remain foreign interop boundaries.
- **OMP shortcut:** a leading `coordinate` enriches the current input with fleet presence and an inspection cue. It does not contact other sessions with prompts.

| Timing/bound | Value |
| --- | --- |
| Presence / healthy poll delay | 10 s / 1 s after a completed cycle |
| Receiver liveness / discovery expiry | 30 s / 120 s |
| Native detail / inspector query deadline | 8 s / 10 s |
| HTTP / advisory disconnect deadline | 10 s / 500 ms |
| Connector retry | Exponential from 2 s; 80–120% jitter; hard 30 s cap |
| Connection diagnostics | Debug-only; 5 s grace checked on failed retries; coalesced across flaps; recovery/reset after 10 healthy seconds |
| Payloads | 64 KiB HTTP/native input; 48 KiB inspection budget |

Connector timeouts and HTTP 409/429/5xx retry and reannounce. HTTP 401/403 retry only after finding a changed token. Other failures end the attempt. Stable recovery resets backoff and error deduplication; one successful poll does not. Background connection failures and optional Claude roster unavailability are debug-only. Detail failures return correlated errors to their requester, not host warnings. Actionable configuration/authentication failures and defects use native UI or headless stderr, deduplicated by operation/code/status independently of connection state. Cancellation is quiet; advisory disconnect remains bounded. Default Effect logs and CLI diagnostics use stderr with safe operation/code/status, never credentials or rejected payloads ([`Logger.LogToStderr`](https://effect.website/docs/v4/api/effect/Logger#LogToStderr)). `/livez` is public; `/readyz` and `/healthz` require an owner bearer. Request deadlines do not bound arbitrary uninterruptible work.

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
| [platform.ts](src/platform.ts) | Effect Node platform composition, scoped CLI stdin, and host/crypto/executable bindings. |
| [main.ts](src/main.ts) | CLI entrypoints and layer composition. |

## Verification

Typechecking uses Effect tsgo. All test suites import `@effect/vitest`; native resource sampling uses `it.live`, while deterministic parsing uses `it.effect`.

```sh
pnpm check
pnpm test
pnpm smoke
```

Smoke uses disposable native sessions without model prompts; it is not deployment/live-provider acceptance. Pi disables automatic catalog refresh. Default source smoke and packed-package acceptance require both Pi and OMP; the production API image explicitly runs inspector-only smoke with `COORDINATOR_SMOKE_NATIVE=false` and does not include harness SDKs. `COORDINATOR_TEST_PI` / `COORDINATOR_TEST_OMP` select existing absolute CLI paths. Source-package OMP needs Bun >=1.3.14; compiled OMP has its own runtime. A loading/discovery failure is a failed native smoke, not deployment proof.

After activation, use `node dist/src/main.js list`, then pass the selected `{"ref": ...}` on stdin to `node dist/src/main.js inspect`; check the intended session and metrics. CLI/installer `--help` list supported options. Versions are pinned in [package.json](package.json); [third-party notices](THIRD_PARTY_NOTICES.md) apply to redistribution.
