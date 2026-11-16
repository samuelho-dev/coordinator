# Coordinator

Inspect existing Pi, OMP, Claude Code, Codex CLI and Hermes sessions across Linux/macOS. Report identity, activity, workspace, machine/container CPU and memory, and session process usage. Optional peer messaging is asynchronous and explicitly authorized.

One Go executable runs the inspector, CLI, MCP bridge and native helpers. Pi/OMP use small JavaScript adapters for host APIs. No transcript collection, token/cost telemetry or remote tool execution.

## Install

Install a pinned release; replace `VERSION` with the required version:

```sh
npm install --global @samuelho-dev/coordinator@VERSION
coordinator --version
```

The package bundles platform binaries; the Go runtime needs no Node server. Installed behavior follows the selected release, not this checkout.

## Configuration

### Inspector

Create a new private server configuration and start the inspector:

```sh
PRIVATE="${XDG_CONFIG_HOME:-$HOME/.config}/coordinator/server"
coordinator init --directory "$PRIVATE" --owner operator
COORDINATOR_CONFIG="$PRIVATE/server.json" coordinator inspector
```

`init` creates `server.json` and a random bearer in `owner.token`; the server stores its SHA-256 hash. The default bind is `127.0.0.1:8787`. Supervise the process yourself. Remote clients require HTTPS; HTTP is allowed only on loopback.

For Kubernetes, use the [Helm chart guide](deploy/helm/coordinator/README.md). It requires an existing Secret, a verified image digest and matching source SHA; it does not install native integrations or TLS routing.

### Hosted OAuth MCP

For a hosted MCP client, optionally add `oauth` to the private inspector server JSON and bind each allowed IdP subject to a dedicated principal without `tokenHash`. Keep the native bearer principal and its SHA-256 `tokenHash` for connectors and health checks. For example (replace the placeholders in private configuration):

```json
{
  "host": "0.0.0.0",
  "port": 8787,
  "principals": [
    {"id": "native", "owner": "operator", "tokenHash": "REPLACE_WITH_NATIVE_TOKEN_SHA256"},
    {"id": "hosted", "owner": "operator", "messaging": true}
  ],
  "oauth": {
    "resource": "https://inspector.example.net/mcp",
    "issuer": "https://idp.example.net",
    "jwksUrl": "https://idp.example.net/.well-known/jwks.json",
    "bindings": [{"subject": "IDP_SUBJECT", "principalId": "hosted"}]
  },
  "hosted": {
    "stateDirectory": "/var/lib/coordinator",
    "callbackHosts": ["callbacks.example.net"]
  }
}
```

The `resource` is the exact public `/mcp` URL; issuer and JWKS URL require HTTPS except for loopback tests. The IdP issues [RFC 9068](https://www.rfc-editor.org/rfc/rfc9068) JWT access tokens with `typ: at+jwt`, RS256 or ES256 signature, exact issuer and resource audience, expiration and an allowlisted subject; `nbf` is honored. Scopes are a space-separated `scope` string or an `scp` string array (Authelia); tokens containing both are rejected. `coordinator:read` is required for MCP access; messaging also requires `coordinator:message` and the bound principal's `messaging: true`. OAuth cannot publish presence, access health or connector routes, enroll, or claim a native caller identity.

Protected-resource metadata advertises `coordinator:read`, adding `coordinator:message` only when an OAuth-bound principal has messaging enabled. Native bearer messaging permissions do not add OAuth scopes. Keep a read-only hosted client's allowed API scope at `coordinator:read`; after a metadata change, refresh the hosted connector's metadata before reconnecting.

The optional `hosted` configuration enables event subscriptions on supported ChatGPT Work surfaces. Use a durable, owner-controlled 0700 directory and exact operator-approved lowercase callback DNS hosts; wildcards and private callback targets are not allowed. The example's OAuth-bound `hosted` principal has messaging enabled, and its IdP token needs both `coordinator:read` and `coordinator:message`. Subscribe to `message.received` with arguments `{"name":"My destination"}` and an approved webhook delivery. Subscriptions and hosted message/reply correlation are persisted; native-only messages and presence are volatile. Callback acceptance does not prove a model processed the message. Replies are authorized to the authenticated account, not verified as originating from the original ChatGPT chat.

Active subscription refresh preserves outstanding messages. Unsubscribe or expiry permanently revokes that lifecycle's delivery and reply authority, even if the same destination is subscribed again. Queued hosted replies survive inspector restart only for the exact original native connector/incarnation; replacements cannot inherit them.

Use an external IdP for authorization code with [S256 PKCE](https://www.rfc-editor.org/rfc/rfc7636), refresh and client registration or pre-registration; Coordinator is not a login server. Follow [MCP authorization](https://modelcontextprotocol.io/specification/2025-06-18/basic/authorization) and configure the public gateway to allow only `/mcp`, `/.well-known/oauth-protected-resource`, and `/.well-known/oauth-protected-resource/mcp`; keep native routes and probes private. Never put tokens, client secrets or other credentials in URLs, logs or Git.

Local token and MCP checks do not establish hosted-client acceptance. After an authorized deployment, connect the actual hosted app through its IdP flow and verify `session.list` returns the expected authorized sessions; separately verify the private native publishing path.

### Clients

Provision the owner's token privately, then connect each host:

```sh
coordinator connect --endpoint https://inspector.example.net --token-file /absolute/private/owner.token
```

Omit `--token-file` for a masked credential prompt. Connection is verified before saving; existing files are never overwritten.

- Default client file: `$XDG_CONFIG_HOME/coordinator/inspector.json`, otherwise `~/.config/coordinator/inspector.json`.
- Select another file with `COORDINATOR_CONNECTOR_CONFIG` or `connect --config`.
- Client JSON uses `inspectorUrl` and either `tokenFile` or `apiToken`. `COORDINATOR_API_URL` / `COORDINATOR_API_TOKEN` override file values; `COORDINATOR_CONFIG` is server-only.
- Private paths must be canonical and absolute, without symlinks; directories/files must be owner-owned and private (0700/0600). Never put credentials in Git, argv or logs.
- Owner keys authorize fleet-wide reads and publishing. Machine IDs are installation labels, not device authentication; any holder of a shared owner key can publish under that owner's machine labels.

## Native host setup

Run as the nonroot native user after client configuration. Choose `omp`, `pi`, `claude`, `codex` or `hermes`:

```sh
TARGET=omp
PRIVATE="${XDG_CONFIG_HOME:-$HOME/.config}/coordinator"
coordinator install --target "$TARGET" --destination "$PRIVATE/$TARGET-host" --dry-run
coordinator install --target "$TARGET" --destination "$PRIVATE/$TARGET-host"
```

The destination must be new and its parent private. The installer stages a binary and host configuration, then prints activation instructions. It never activates integrations, grants hook trust or starts services.

| Host | Activation | Presence publisher |
| --- | --- | --- |
| Pi / OMP | Load the staged extension using the printed command. OMP's generated `mcp.json` enables MCP tools separately. | Adapter-owned Go child |
| Claude Code | Use the printed `claude --plugin-dir` command; approve hooks/MCP. | Supervise generated `connector.sh` |
| Codex CLI | Merge generated hooks and `mcp.toml` into native settings; approve hooks in `/hooks`. | Supervise generated `connector.sh` |
| Hermes | Merge generated hooks and MCP settings into the selected profile. | Supervise generated `connector.sh` |

`mcp.sh` is the stdio reader bridge. Hook capture is local; publishing requires the connector and credentials. Machine identity is created automatically on first native capture. Building or installing artifacts never starts a publisher.

## Use

```sh
coordinator list
coordinator recipients
printf '%s\n' '{"ref":{"machineId":"installation-uuid","harness":"omp","nativeSessionId":"session-id"}}' | coordinator inspect
```

Copy the complete `ref` from a listed session, including `nativeAgentId` when present. Pi/OMP also provide `/coordinator list` without starting a model turn. Use `coordinator recipients` or `recipient.list` to discover available native and subscribed hosted destinations; native sessions remain separately available for metrics.

Session lists return `{machines, sessions, nextCursor}`. CLI and slash-list displays drain pages; Pi's `coordinator_session_list` and MCP `session.list` preserve pagination with `limit` (1–100), optional `cursor`, `machineId` and `repository` (the exact remote Git common-directory path). `recipient.list` also accepts `limit` and optional `cursor`. `session.inspect` accepts the complete ref. HTTP MCP is at `/mcp`; `coordinator mcp` provides stdio.

### Peer messaging

Messaging is off by default. Enable it independently on the server principal (`init --messaging` for a new server), client (`connect --messaging`) and staged integration (`install --messaging`). These flags do not grant host trust or alter existing server permissions.

```sh
coordinator message send <<'JSON'
{"to":{"kind":"session","session":{"machineId":"installation-uuid","harness":"pi","nativeSessionId":"session-id"}},"text":"Please review this change","requestId":"request-1"}
JSON
coordinator message get <<'JSON'
{"messageId":"message-id"}
JSON
```

Pi/OMP support native send and correlated reply tools. Claude's receiver is a development preview requiring the printed development-channel activation command. Hermes requires its staged plugin, `plugins.entries.coordinator.allow_gateway_injection: true`, the same client configuration in the gateway, and `/coordinator enable` in the actual session.

Codex uses the normal MCP bridge by default, with unbound send/get. To opt into messaging with a shared, already running Codex app-server, resolve its chosen listener symlink and stage with the actual socket path:

```sh
SOCKET="$(realpath /chosen/server.sock)"
coordinator install --target codex --messaging --codex-socket "$SOCKET" --destination "$PRIVATE/codex-host"
```

Merge the generated settings and approve hooks. The resolved socket must be owned by the native user, mode 0600, in a private owned directory; the exact thread must be loaded on that shared host. An unsafe socket, embedded server, or unreachable server fails closed. Installation does not change socket permissions or start, restart, resume, or create a server.

Peer text is not user approval or system instruction. Native receivers reply using the original message ID as `replyId`; host tools supply exact native caller identity. `accepted` means transport admission, not completion. Delivery expires after 10 minutes; records last up to 30 minutes. Native-only message records and native discovery are in memory and are lost on inspector restart; hosted message and reply correlation is persisted.

`recipient.list` returns discriminated refs: `kind: "session"` with the complete native session ref, or `kind: "subscription"` with `subscriptionId` for an active hosted callback. Native `from` identifies the verified bound session; an OAuth sender is attributed to the authenticated principal, not a verified ChatGPT chat. Select native peers by exact ref, not a shared credential name, project, cwd or title. If the intended parent has no receiver, use the existing task terminal or issue rather than another session.

## Metrics and provenance

Measurements come from the agent's own machine/VM or Linux cgroup, never the inspector. Lists read published snapshots; inspection refreshes native getters and process samples. Missing or stale counters stay explicitly unavailable/stale, not zero.

CPU/RSS capacity percentages use that execution scope's capacity. Shared process readings overlap: **never sum shared session rows or repeated machine readings**. RSS is not unique physical memory; macOS used memory includes inactive cache and is not memory pressure. [Wire types](internal/wire) define detailed fields.

### Coordinated migration

Inspectors, adapters, MCP bridges and connectors require compatible strict contracts; MCP negotiation does not negotiate tool schemas. Update them together, explicitly restart affected services and reload native hosts/new sessions. Existing sessions retain loaded code. Preserve existing broker/PVC state; a source update is not authorization to delete it.

The beta.15 recipient contract changes `message.send.to` and message `from`/`to` from native session refs to discriminated recipient refs. Deploy the inspector and native readers/adapters together; beta.14 callers are not compatible with this message shape.

## Source

`cmd/` contains the executable, builder, schema generator and smoke commands. `internal/` contains runtime (`coordinator`), contracts (`wire`), observations (`observe`) and embedded assets/tests (`integrations`). Top-level `integrations/` contains Pi/OMP TypeScript sources and generated bindings.

Requires Go 1.25+ and the pinned pnpm version in `package.json`:

```sh
pnpm install --frozen-lockfile
go run ./cmd/build
pnpm check
go test -race ./...
go vet ./...
```

The builder generates schemas, bundles adapters and builds the current platform. `COORDINATOR_BUILD_TARGETS=linux-x64,linux-arm64` selects Linux targets; Darwin builds require macOS/cgo. `go run ./cmd/smoke` exercises installed native hosts; `go run ./cmd/package-smoke path/to/package.tgz` checks a packaged installation.

CI seeds private configuration through a Docker volume, then checks the production image as UID1000 with a read-only root, no capabilities and no privilege escalation; the DinD runner needs no host `chown` or shared bind-mount path.

### First npm publication

Publish only the exact CI-accepted tarball with owner authorization, reviewed contents and required license text in `LICENSE`. First publication requires an authorized npm login and 2FA; subsequent releases use the protected `npm-public` trusted-publisher environment. See [npm release](.github/workflows/npm-release.yaml) and [server/chart release](.github/workflows/release.yaml) workflows. Publication does not deploy, activate hosts or register marketplaces.
