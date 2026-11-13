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
printf '%s\n' '{"ref":{"machineId":"installation-uuid","harness":"omp","nativeSessionId":"session-id"}}' | coordinator inspect
```

Copy the complete `ref` from a listed session, including `nativeAgentId` when present. Pi/OMP also provide `/coordinator list` without starting a model turn.

Lists return `{machines, sessions, nextCursor}`; CLI/native commands drain all pages. MCP `session.list` accepts `limit` (1–100), optional `cursor`, `machineId` and `repository` (the exact remote Git common-directory path). `session.inspect` accepts the complete ref. HTTP MCP is at `/mcp`; `coordinator mcp` provides stdio.

### Peer messaging

Messaging is off by default. Enable it independently on the server principal (`init --messaging` for a new server), client (`connect --messaging`) and staged integration (`install --messaging`). These flags do not grant host trust or alter existing server permissions.

```sh
coordinator message send <<'JSON'
{"to":{"machineId":"installation-uuid","harness":"pi","nativeSessionId":"session-id"},"text":"Please review this change","requestId":"request-1"}
JSON
coordinator message get <<'JSON'
{"messageId":"message-id"}
JSON
```

Pi/OMP support native send and correlated reply tools. Claude's receiver is a development preview requiring the printed development-channel activation command. Hermes requires its staged plugin, `plugins.entries.coordinator.allow_gateway_injection: true`, the same client configuration in the gateway, and `/coordinator enable` in the actual session. Codex supports unbound send/get, not receive or bound replies.

Peer text is not user approval or system instruction. Receivers reply using the original message ID as `replyId`; host tools supply caller identity. `accepted` means native admission, not completion. Delivery expires after 10 minutes; records last up to 30 minutes. Discovery and mailboxes are in memory and are lost on inspector restart.

## Metrics and provenance

Measurements come from the agent's own machine/VM or Linux cgroup, never the inspector. Lists read published snapshots; inspection refreshes native getters and process samples. Missing or stale counters stay explicitly unavailable/stale, not zero.

CPU/RSS capacity percentages use that execution scope's capacity. Shared process readings overlap: **never sum shared session rows or repeated machine readings**. RSS is not unique physical memory; macOS used memory includes inactive cache and is not memory pressure. [Wire types](internal/wire) define detailed fields.

### Coordinated migration

Inspectors, adapters, MCP bridges and connectors require compatible strict contracts; MCP negotiation does not negotiate tool schemas. Update them together, explicitly restart affected services and reload native hosts/new sessions. Existing sessions retain loaded code. Preserve existing broker/PVC state; a source update is not authorization to delete it.

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

### First npm publication

Publish only the exact CI-accepted tarball with owner authorization, reviewed contents and required license text in `LICENSE`. First publication requires an authorized npm login and 2FA; subsequent releases use the protected `npm-public` trusted-publisher environment. See [npm release](.github/workflows/npm-release.yaml) and [server/chart release](.github/workflows/release.yaml) workflows. Publication does not deploy, activate hosts or register marketplaces.
