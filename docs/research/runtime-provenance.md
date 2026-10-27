# Generic terminal provenance

## Contract

Session presence, list and inspect expose optional `terminal: { name, version? }`:

- `name` is the source process's `TERM_PROGRAM`, without product-name normalization or a whitelist.
- `version` is its optional `TERM_PROGRAM_VERSION`.
- No valid name means unknown, even when a version, `TERM`, vendor-specific variables, hostname or container metadata is present.
- Values are bounded to 4096 characters, must be nonblank and cannot contain control characters. Invalid metadata is omitted rather than breaking native publishing.

There are no product-specific detectors, pane/session IDs, aliases, runtime discovery calls or terminal handshakes. The earlier product-specific associations have been removed rather than retained as compatibility paths. Hook metadata is captured at the hook source; readers/connectors cannot substitute their own terminal for a cached observation.

## Why this detects new terminals without code changes

Tern documents `TERM_PROGRAM=tern` and `TERM_PROGRAM_VERSION` for every pane, including session-daemon and `tern remote` panes: [Tern environment and opt-outs](https://docs.stencil.so/tern/protocol/handshake.html#environment-and-opt-outs). Those values pass through the same generic collector as any other name; Tern is not a branch in Coordinator.

Other applications also set these markers. For example, Orca sets them in its [local PTY environment](https://github.com/stablyai/orca/blob/v1.4.206/src/main/providers/local-pty-spawn-environment.ts#L26-L31) and [daemon PTY environment](https://github.com/stablyai/orca/blob/v1.4.206/src/main/daemon/pty-subprocess/spawn-environment.ts#L142-L145). This is evidence of the generic contract, not a reason for a separate adapter.

## Boundaries

Environment metadata is an inherited claim, not authentication, proof of process ancestry or validation of a live terminal. SSH can remove it, and multiplexers can replace or preserve it. A missing marker stays unknown; Coordinator does not guess. `TERM=xterm-256color` describes terminal capabilities rather than the application and is not a name fallback.

Terminal identity is distinct from execution environment. CPU/RAM still comes from the actual local host/cgroup under `machine.resources`; a terminal name does not identify a container image, remote runtime or resource owner.

No terminal input/output queries are issued, so detection cannot consume native host input or interfere with rendering. Product-independent OSC 7501 status reporting remains separate in `program-status.ts`; stock OMP continues to own its reporter.

## Implementation and cutover

- [`src/terminal.ts`](../../src/terminal.ts) reads only the two standard markers through Effect `Config` and the bounded shared schema.
- [`src/inspection.ts`](../../src/inspection.ts) defines the generic wire group and rejects removed product-specific groups and unknown nested fields.
- [`src/plugin.ts`](../../src/plugin.ts) captures native host identity; [`src/native.ts`](../../src/native.ts) captures external hook identity before caching.
- Missing versions are omitted, not assigned `undefined`: Effect's `Schema.toCodecJson` otherwise encodes explicit `undefined` as `null`. The JSON wire regression protects this boundary.
- Cache version 4 uses a separate filename namespace. Earlier records are ignored and preserved, so removing old fields cannot block new capture on an old session's cache filename.
- `0.3.0-beta.8` is a breaking wire change requiring matching API, publishers and readers. Existing credentials and installation identity remain valid; runtime dependencies and stock hosts are unchanged.

## Verification scope

Permanent coverage exercises an arbitrary future terminal name, Tern's documented marker values, absent versions through the JSON codec, invalid/missing markers, ignored vendor hints, excluded credentials, strict legacy-field rejection, source-versus-connector attribution and the old-cache cutover.

Typecheck, build and all 125 tests passed. Linux full native Pi/OMP acceptance preserved generic identity through published presence, inspection and displayed `/coordinator list` JSON. Targeted stock OMP acceptance on Mac reported only `name` and `version` from its standard markers. Both used isolated inspectors without model turns or production changes.

A stock OMP marker fixture also reported `{"name":"tern","version":"0.4.3"}` through list, inspect and displayed JSON. This verifies generic handling, not operation of a live Tern application. The broader Mac smoke previously stopped at its initial owned-process-count assertion; that separate resource assertion was not weakened or rerun for this change.
