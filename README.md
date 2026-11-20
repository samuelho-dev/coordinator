# Coordinator

Self-hosted session inspection and opt-in messaging for Pi, OMP, Claude Code, Codex and Hermes on Linux/macOS.

## Functionality

- Discover sessions across machines; inspect model, activity, workspace and Git context.
- Read machine/container and session CPU/memory usage.
- Discover native messaging recipients, send messages, reply and check delivery.

No transcript collection, token/cost telemetry or agent task execution.

## Install

Requires Node.js 22.19+. Platform binaries are bundled; replace `VERSION` with a pinned release:

```sh
npm install --global @samuelho-dev/coordinator@VERSION
coordinator --version
```

## Configuration

### Inspector

Create private credentials and start the server:

```sh
PRIVATE="${XDG_CONFIG_HOME:-$HOME/.config}/coordinator"
coordinator init --directory "$PRIVATE/server" --owner operator
COORDINATOR_CONFIG="$PRIVATE/server/server.json" coordinator inspector
```

The default is `127.0.0.1:8787`. Run under your own process supervisor. For remote access, provide HTTPS and privately provision `owner.token` to each client machine. Kubernetes: [Helm guide](deploy/helm/coordinator/README.md).

### Clients

In another terminal, connect a local client:

```sh
PRIVATE="${XDG_CONFIG_HOME:-$HOME/.config}/coordinator"
coordinator connect --endpoint http://127.0.0.1:8787 --token-file "$PRIVATE/server/owner.token"
```

For remote clients, use HTTPS and a privately provisioned token. Omit `--token-file` for a masked prompt. Existing files are never overwritten.

Client config: `~/.config/coordinator/inspector.json` (respects `XDG_CONFIG_HOME`); override with `COORDINATOR_CONNECTOR_CONFIG`. Fields are `inspectorUrl` and either `tokenFile` or `apiToken`; environment overrides are `COORDINATOR_API_URL` / `COORDINATOR_API_TOKEN`. `COORDINATOR_CONFIG` is server-only.

Use canonical, user-owned paths: 0700 directories, 0600 files. Tokens grant owner-wide fleet access; machine IDs are not authentication. Keep credentials out of URLs, logs and Git.

## Native host setup

After connecting, run as the nonroot host user. Choose `omp`, `pi`, `claude`, `codex` or `hermes`:

```sh
TARGET=omp
PRIVATE="${XDG_CONFIG_HOME:-$HOME/.config}/coordinator"
coordinator install --target "$TARGET" --destination "$PRIVATE/$TARGET-host"
```

Use a new destination; add `--dry-run` to preview. Follow the printed activation instructions:

| Host | Activation |
| --- | --- |
| Pi / OMP | Load the printed extension command in a fresh host. Configure OMP MCP separately. |
| Claude Code | Use the printed `claude --plugin-dir` command; approve hooks/MCP. |
| Codex | Merge generated hooks and `mcp.toml`; approve hooks in `/hooks`. |
| Hermes | Merge generated hooks/MCP and approve commands. |

Pi/OMP adapters manage their Go child; for other hosts, supervise `connector.sh`. Installation neither starts services nor grants trust. Activate loaded code through host controls without terminating human work.

## Use

```sh
coordinator list
coordinator recipients
printf '%s\n' '{"ref":{"machineId":"installation-uuid","harness":"omp","nativeSessionId":"session-id"}}' | coordinator inspect
```

Copy complete refs from discovery, including `nativeAgentId`. Pi/OMP also offer `/coordinator list` without a model turn.

### MCP

After client configuration, add Coordinator to a standard stdio MCP client:

```json
{"mcpServers":{"coordinator":{"command":"coordinator","args":["mcp"]}}}
```

For remote MCP, use `https://inspector.example.net/mcp` with the [OAuth setup below](#hosted-oauth-mcp).

## Messaging

Messaging is off by default. Add `--messaging` when running **each** of `init`, `connect` and `install`; host approval is still required.

```sh
coordinator message send <<'JSON'
{"to":{"kind":"session","session":{"machineId":"installation-uuid","harness":"pi","nativeSessionId":"session-id"}},"text":"Please review this change","requestId":"request-1"}
JSON
printf '%s\n' '{"messageId":"message-id"}' | coordinator message get
```

Use the recipient's discovered `ref` as `to`. Native reply tools take the original message ID as `replyId`; CLI senders are unbound.

- **Claude Code:** receiving needs the printed development-channel activation and trust; this is not Claude.ai push support.
- **Codex:** receiving/bound replies need `--codex-socket /canonical/private/server.sock` at installation and an already-running shared app-server with the exact thread loaded. The socket must be user-owned, mode 0600, in a private directory. Installation does not start/resume a server or thread.
- **Hermes:** enable the generated native plugin and `coordinator` toolset, configure the gateway client, grant `plugins.entries.coordinator.allow_gateway_injection: true`, then run `/coordinator enable`. Tools may be deferred behind `tool_call`; stop with `/coordinator disable`.

Peer text is not approval; `accepted` is not completed work. Native-only messaging state is volatile.

### Hosted OAuth MCP

Hosted clients require an external OAuth IdP with authorization code + PKCE. In `server.json`, retain the native bearer principal and add an OAuth principal without `tokenHash`. Configure `oauth.resource` as the public MCP URL, `issuer`, `jwksUrl` and subject-to-principal `bindings`. See [configuration fields](internal/wire/types.go).

JWT access tokens must match the resource/issuer and allowed subject, with `coordinator:read`; messaging also requires `coordinator:message` and principal permission. Use `scope` or `scp`, not both. OAuth cannot publish native presence or claim native identity.

Expose only `/mcp` and its [protected-resource metadata](https://modelcontextprotocol.io/specification/2025-06-18/basic/authorization) publicly; keep connectors/probes private.

## Metrics and provenance

Counters come from the agent's machine/container, not the inspector. Missing/stale readings remain explicit. Shared CPU/RSS readings overlap: **do not sum them**.

Use compatible inspector/client versions; MCP does not negotiate result schemas. [Wire contracts](internal/wire).

## Source

Requires Go 1.25+ and the pinned pnpm version:

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm check
go test -race ./...
go vet ./...
```

Darwin builds require macOS/cgo. Native smoke: `pnpm smoke`; package smoke: `pnpm smoke:package path/to/package.tgz`.
