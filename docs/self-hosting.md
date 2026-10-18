# Coordinator self-hosting operator guide

## Status and boundaries

This guide describes the prepared private semantic cutover, **not a published, deployed or accepted release**. Coordinator is one application/source tree: the broker coordinates durable work, the isolated runner executes it, the plugin integrates ordinary Pi/OMP, and `src/agent/` controls owned SDK sessions over MCP. Both service roles require separate PostgreSQL18 databases/logins. No command below is automatic deployment or authorization to mutate an existing installation.

The [README verification record](../README.md#verification-status) distinguishes pre-consolidation observations from bounded phases1–3 proofs: Node24 full check,13 database/backup tests,39 host-group tests, real paired recovery, actual pinned Pi/OMP stock-host fixture without an external provider, and actual UID1000001/cap0 isolation with zero model-provider requests. Phase4 final coordinated acceptance is pending. Exact-source normal CI, signed release artifacts and actual operator acceptance remain separate gates; local checks are not deployment approval.

Historical [beta.1](https://github.com/samuelho-dev/coordinator/actions/runs/38373742895) and [beta.2](https://github.com/samuelho-dev/coordinator/actions/runs/38380085130) publication attempts failed chart signing/attestation respectively; neither incomplete artifact may be promoted. [Beta.3 publication](https://github.com/samuelho-dev/coordinator/actions/runs/38388115672) and [beta.4 publication](https://github.com/samuelho-dev/coordinator/actions/runs/38529041023) subsequently completed privately. Their immutable source/image/chart coordinates do not certify the unpublished beta.5 semantic cutover; no overwrite/resume or old-green bypass is permitted.

MIT is selected in [LICENSE](../LICENSE). Source, image and chart remain private until a separate owner release/visibility decision. [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) records incomplete redistribution review; inspect source history, dependencies, native binary and notices before any public release. Current image builds no longer include the historical complete Git source bundle; old images/bundles need their own audit before redistribution. A license label or metadata spot-check does not certify that audit. Do not publish credentials, protected recovery artifacts or operator configuration.

Broker and each enabled runner require separate PostgreSQL databases/logins, each with one session-advisory-fenced owner; runner filesystem state additionally has kernel flock. No pool, reconnect/retry, HA, safe replay of uncertain agent work, automatic promotion or rollback is promised. The generic chart requires external PostgreSQL18 and provisions no database server. Start broker-only, then enable the runner after independent database/TLS/repository/model/isolation prerequisites are ready. This is greenfield PostgreSQL initialization: no alternate storage backend, importer or historical migration chain is supported.

### Historical homelab context, not installation defaults

The earlier authorized target was the existing coordinator release and broker/homelab runner endpoints. CNPG cloudnative-pg/coordinator-beta retained its private internal RW service; separately credentialed broker/runner/reserved-beta roles were coordinator/coordinator_runner/coordinator_beta. These operator facts do not authorize this documentation task to convert live data or activate a release. Preserve public ca.crt, hostname verification and scoped HBA/Cilium identities. Historical Mac inspection found no Coordinator runner; host plugins access the broker without direct PostgreSQL. Existing broker/plugin/Tailscale/Orca routes and unrelated ACL grants remain out of scope.

Preserve the existing owned `Dev/coordinator-beta` item: live broker uses `production-database-username=coordinator`/`production-database-password`, homelab runner uses `runner-database-username=coordinator_runner`/`runner-database-password`, reserved inactive beta retains `database-username=coordinator_beta`/`database-password`. Three independent pairs plus five preexisting tokens are11 core fields; `backup-access-key-id`/`backup-secret-access-key` make13 total. Existing production enrollment bearers and broker backup fields remain selected from `Dev/coordinator` through per-field overrides. Never truncate/rotate existing tokens or export credentials to Git/transcripts. CNPG backup `coordinator-beta-backup` uses scoped B2 keys at `s3://ai-dev-env/postgres/coordinator-beta/`, endpoint `https://s3.us-east-005.backblazeb2.com`; provisioning evidence does not prove current backup/restore.

Hindsight is **PostgreSQL with pgvector/selected vector extension**, or embedded **pg0 PostgreSQL** for local use, not SQLite; current documentation also names Oracle AI Database as an enterprise alternative. Its public product/private operator overlay is a useful release-ownership comparison, not authorization to reuse its shared database, credentials or automatic worker retries. See [primary-source Hindsight findings](deployment-research.md#postgresql-sqlite-and-embedded-pg0), [official storage](https://hindsight.vectorize.io/developer/storage) and [external PostgreSQL installation guidance](https://hindsight.vectorize.io/developer/installation#postgresql).

The database's required `homelab-cp` affinity/control-plane NoSchedule toleration excludes bulk CI/DIND on ordinary workers, not all CI: the scoped capacity-one `gha-rs-maintenance` remains on CP. Verify actual placement and accept its constraints before live acceptance. One CNPG instance requires `primaryUpdateStrategy: unsupervised`; local-path shares CP root I/O with etcd/other workloads, so this is neither physical isolation nor HA/automatic failover.

**Cutover order:** reviewed policy/trust → scoped source/artifact access and preservation-only credential provisioning/public CA/external PostgreSQL readiness → exact-source normal CI and verified private release → reviewed suspended desired pins → all-client drain/configuration and protocol replacement → one initial schema on fresh separate databases → explicit restart/reload → private-route/rotation/recovery acceptance. Nothing in this guide performs or authorizes automatic activation; existing live data must not be treated as an input to an unsupported conversion workflow.

**Policy/provisioning evidence:** private infrastructure [PR1297](https://github.com/samuelho-dev/ai-dev-env/pull/1297) passed normal CI and merged at `df650e09c7cfc98920bbf9457f20fcf9a1be00d5`, without beta activation. Exact-main-push [normal CI38362192158](https://github.com/samuelho-dev/ai-dev-env/actions/runs/38362192158) succeeded. Approved Dev bootstrap dispatched once at that SHA; [run38364590812](https://github.com/samuelho-dev/ai-dev-env/actions/runs/38364590812) succeeded. Bounded results prove nine independent `Dev/coordinator-beta` fields saved/read back: database-username/password, HOMELAB_SESSION_TOKEN, HOMELAB_RUNNER_TOKEN, HOMELAB_RUNNER_CONTROL_TOKEN, REVIEWER_TOKEN, ARCHIVE_WRITER_TOKEN and backup-access-key-id/backup-secret-access-key. User `coordinator_beta`, independent256-bit password/five bearer-control tokens and B2 key pair were stored; vault writes and actual B2 authentication/prefix `ai-dev-env/postgres/coordinator-beta/` were proven with deleteFiles/listAllBucketNames/listBuckets/listFiles/readFiles/writeFiles capabilities. No credential values were printed. This is provisioning/backend-scope evidence, not Reader/package access, database creation/off-cluster restore or deployment.

**Promotion environment:** `coordinator-beta-promotion` exists in the infrastructure repository; the observed custom deployment branch policy accepts exactly branch `main`, not tags, with no required reviewers. Main remains unprotected: record owner review rather than claim enforced review. Provision the absent private source Reader as an **environment-only** Secret here, never as a repository Secret; private-package Actions read grants remain external. Corrective workflow preparation separates contents-only “Coordinator beta proposal data” from activated-only “Coordinator beta trusted release” in this environment, removes unnecessary promotion `workflow_dispatch`, and binds immutable event-main `github.sha` plus same-event PR head using independent fixed fetches. This does not prove a live promotion. The existing manual main/dev 1Password bootstrap is unchanged.


## 1. Ownership and prerequisites

Use an operator-owned Kubernetes namespace, deployment Git repository and private secret backend. You need authorized access to the exact private source/image, Linux amd64 nodes, Helm (release workflow pins 3.19.0), kubectl, Node **24.21.0**, pnpm **11.9.0**, and sufficient Kubernetes policy permissions for the documented initializer/supervisor capabilities. Argo CD is the canonical reconciliation path, but the initial disposable Helm install below requires no Argo/provider CRD.

An operator image needs verified TLS443 to `deb.debian.org`, `apt.postgresql.org`, `www.postgresql.org`, npm, GitHub and required registries. Dockerfile bootstraps a missing CA bundle from Node24.21.0 trusted roots, uses HTTPS Debian sources and the official **signed PGDG** bookworm repository, and installs **postgresql-client-18 only**, not a server. It preserves TLS and apt signature verification. Native `/usr/bin/pg_dump` and `/usr/bin/pg_restore` are required by backup/recovery. Build-network access is separate from pod NetworkPolicy; unsigned local images are never trusted release evidence.

| Boundary | Owner and required input |
| --- | --- |
| Source | Source owner reviews changes and requires exact-source CI. Workflow YAML does not configure branch protection. |
| Release | Release owner selects the successful trusted main CI run and verifies its existing image. Default is prepare-private; actual same-source private publication is required before GitOps promotion. |
| Deployment | Operator PR owns chart artifact, image repository/digest, full source SHA, nonsecret values, maintenance and acceptance. |
| Admission | Cluster owner trusts exact image producer/OIDC issuer, signature subject digest and attested repository/commit; reject unsigned or mismatched artifacts. |
| Secrets | Operator provisions least-privilege credentials outside plaintext Git, encrypted at rest with restricted Secret/Pod access. |
| TLS/network | Operator provides private verified HTTPS routes, certificates/trust, DNS and default-deny policies with explicit necessary flows. |
| Storage | Operator owns external PostgreSQL18, separate broker/runner databases and scoped logins, verified TLS, durable server backup/recovery and backup/runner-work PVCs with Retain/off-cluster protection. Work files require durable private filesystem semantics. |
| Host clients | Each host owner installs/reloads the exact Coordinator client with the supported SDK; Kubernetes cannot reload a running TUI. |

The investigation observed unprotected source and infrastructure `main` branches and a rulesets API 403 under the private plan. Those are historical observations, not proof of current settings or permission to change visibility. Configure review/required checks with an authorized owner when the platform/plan allows; if unreachable, record the missing enforcement and use explicit owner review without claiming branch enforcement. Do not bypass admission or change a plan/repository's visibility as an installation side effect.

The product chart creates ordinary Deployments, ClusterIP Services and PVCs only. It has no common-chart dependency, ingress/gateway, namespace, NetworkPolicy, HPA or provider CRD. The maintainer's homelab overlay (Tailscale/Cilium/MetalLB/1Password/Kyverno/Garage/Mac identities) is an example of a downstream integration, not an upstream dependency. Keep provider-specific manifests and trust identities in your deployment repository. Do not apply the historical homelab chart defaults to an independent installation.

### Supported identity and version matrix

* Broker/isolated-runner image: Linux **amd64**, Node **24.21.0**; checksum-pinned OMP **18.6.1** at `/usr/local/bin/omp`.
* Supported SDK hosts: Pi **1.0.4**, OMP **18.6.1**. Pi CLI is `/app/node_modules/.bin/pi` in the stock image; prior-tree host smoke used that real CLI. Configure the actual absolute launcher and prove current-source repository/model/provider/isolation before enabling Pi work. Derivative images require their own verified provenance.
* Broker/runner command transport: stateless MCP **2026-07-28**, official client **2.3.1**. Plugin-owned interactive agent session control uses stateful **2025-11-25**. SDK session control is not broker coordination; no mismatched command clients/servers.
* Chart/app version `0.2.0-beta.5` is prepared but unpublished; **Effect4.0.3 is stable**, while Prisma **8.0.0-rc.22** and `@prisma/orm-postgres` **8.0.0-rc.17** are release candidates. This remains private beta, not stable/public certification.
* Full 40-hex source SHA, image `sha256:` digest and chart artifact checksum/OCI digest identify different things. A package version or configured readiness source header does not prove provenance; verify the immutable producer/source linkage.
* Both interfaces use MCP Streamable HTTP, not deprecated standalone HTTP+SSE. Commands are POST-scoped JSON/SSE, including subscriptions/listen, without sessions or GET stream; stateful agent control remains separate. Preserve incremental delivery/cancellation/backpressure and per-message bounds, not whole-stream buffering. Prior-tree SDK HTTP/SSE/auth/abort and concurrent progress/cleanup observations are historical; repeat current-source and private-route gates. Record actual progress/completions/latency, not configured concurrency or promised sustained capacity ([official specification](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http.md)).

## 2. Acquire one exact release, privately

From an authorized checkout of the intended full source commit, use the existing lockfile; do not upgrade the SDKs or global user profiles:

```sh
pnpm install --frozen-lockfile
pnpm build
```

After actual beta.5 publication, obtain `release-manifest.json`, `coordinator-0.2.0-beta.5.tgz`, license/notices and verification evidence from the release owner. Source SHA/image digest/CI producer/platform must agree; verify archive SHA-256 against `chartArchiveSha256`, not an OCI digest. These artifacts are not claimed to exist yet.

The [release workflow](../.github/workflows/release.yaml) must accept the exact source beta version and pass same-source supplementary/chart/image gates before private preparation. `prepare-private` is only artifact preparation, not a published/promotable release. The release owner must subsequently complete actual same-source `publish` at existing private visibility before beta GitOps promotion. Do not borrow old successful CI or infer a release from this document.

`publish` publishes the chart and GitHub release at **existing visibility**, refuses an existing version, signs the chart using the release workflow identity and records the real OCI digest. It never changes visibility or signs the image as a new producer. Public visibility and complete legal/history review require a separate owner action; do not infer public readiness from `publish` success. Do not invent an OCI install command until an actual published, verified chart coordinate is available. For preparation/disposable installation, use the verified local chart archive or exact-source chart directory.

### One declarative schema owner

`prisma/contract.prisma` owns all broker, `dispatch_queue` and runner journal tables (`runner_tasks`/`runner_events`). One initial attested PostgreSQL migration replaces the prior timeline. Committed generated contracts/types, `prisma.config.ts` and that migration are immutable image inputs. Apply the same exact schema separately to fresh broker and runner databases. Runtime startup checks generated storage/profile identity, concrete tables, dispatch and recovery readiness; it does not apply DDL or inspect an Effect migration tracker. Prisma tooling owns migration-history verification.

Generated `src/generated/database/types.ts` database/ID/select/insert/update types are authoritative internally. Typed Kysely results are not decoded again. Decode unknown network/config/filesystem and serialized JSON once at those seams; schema codecs never replace authorization/CAS/fingerprints/recovery fences.

The owner's generic stable Effect4/native ESM fix [PR70](https://github.com/samuelho-dev/prisma-effect-kysely/pull/70) and release preparation [PR71](https://github.com/samuelho-dev/prisma-effect-kysely/pull/71) completed normal publication: [Release38345704625](https://github.com/samuelho-dev/prisma-effect-kysely/actions/runs/38345704625) succeeded and [`prisma-effect-kysely9.0.2`](https://registry.npmjs.org/prisma-effect-kysely/9.0.2) is published. Coordinator's root exact devDependency/lock pin, install, actual generation and native ESM import succeeded. Do not hand-edit generated imports or use old Effect3 infra pins. `pnpm check` runs actual `pnpm db:generate`, imports the generated ESM index and checks types. Normal CI compares all regenerated `src/generated` assets with committed output and fails on drift before its real-PostgreSQL/native gates; generator publication alone is not beta certification.


For schema authors, use the installed tooling in the intended source tree; operators do not generate a new schema during installation:

```sh
pnpm db:generate
# Initial greenfield schema emission, only when authoring this cutover:
pnpm exec prisma migration plan --from @empty --name initial
```

Commit generated contracts/types and the emitted attested initial migration together; reproduce generation in CI. Never hand-edit generated output or application SQL, use destructive reset/db push, or generate against a running service. Before schema application, have the secret manager inject the selected fresh target's URL as `COORDINATOR_DATABASE_URL` into the migration process environment; never expand the value into argv or logs. Schema application is only:

```sh
NODE_EXTRA_CA_CERTS="$PRIVATE/ca.crt" \
  node --import tsx scripts/migrate.ts
```

The script acquires the PostgreSQL session owner and applies native `db migrate --no-interactive` to the exact `contract.storage.storageHash`, then verifies both role schemas. `DatabaseService` checks generated storage/profile identity, dispatch and recovery readiness through typed Kysely queries; Prisma tooling owns migration-history verification. Database failures use `DatabaseError`, with diagnostics and credentials withheld. Run separately for each fresh database with every owner stopped; no live-owner PreSync or skip flag.


## 3. Create private schema-valid configuration

`config.json` in each role Secret is the authoritative existing [BrokerConfig/RunnerConfig](../src/contracts.ts), decoded with excess-property rejection. There is no chart-specific enrollment schema. Owners/principals/machines/runners use your own opaque string identifiers with short ID brands, not a custom regex format; repositories use lowercase `github.com/owner/repository`; hashes are lowercase SHA-256, pins are full lowercase 40-hex. Enroll only required roles and repository scopes. A no-runner broker needs no task repository or maintainer source checkout.

The following generates a **broker-only** initial configuration and a separate session principal for no-dispatch acceptance. Set `PRIVATE` to a new absolute local private directory and export your chosen `OWNER_ID`, `SESSION_ID`, `MACHINE_ID`, and real private HTTPS `BROKER_URL` authority (no URL credentials/query/path). These are operator inputs, not sample live identities. Keep shell tracing off; tokens are never arguments or stdout.

```sh
umask 077
: "${PRIVATE:?absolute new private directory}" "${OWNER_ID:?}" "${SESSION_ID:?}" \
  "${MACHINE_ID:?}" "${BROKER_URL:?private HTTPS authority}"
export PRIVATE OWNER_ID SESSION_ID MACHINE_ID BROKER_URL
mkdir -m 700 "$PRIVATE"
node --import tsx --input-type=module <<'JS'
import { Effect, Schema } from 'effect';
import { Hex } from 'effect/encoding';
import { Platform } from './src/platform.ts';
import { BrokerConfig, PluginConfig, CommandError } from './src/contracts.ts';
const e = process.env;
await Effect.runPromise(Effect.gen(function*() {
  const p = yield* Platform;
  if (!p.path.isAbsolute(e.PRIVATE) || p.path.resolve(e.PRIVATE) !== e.PRIVATE ||
      (yield* p.fs.realPath(e.PRIVATE)) !== e.PRIVATE)
    return yield* new CommandError({message:'PRIVATE must be canonical and absolute'});
  const token = Hex.encode(yield* p.crypto.randomBytes(32));
  const hash = yield* p.digest(token);
  const broker = Schema.encodeUnknownSync(Schema.fromJsonString(BrokerConfig))({
    host:'0.0.0.0',port:8787,
    principals:[{id:e.SESSION_ID,owner:e.OWNER_ID,tokenHash:hash,roles:['session'],repos:[]}],
    backupDirectory:'/backups',archiveRetentionMs:86400000,backupRetentionMs:259200000,
    repositoryIds:{}});
  const plugin = Schema.encodeUnknownSync(Schema.fromJsonString(PluginConfig))({
    brokerUrl:e.BROKER_URL,tokenFile:`${e.PRIVATE}/session.token`,
    machineId:e.MACHINE_ID,stateDirectory:`${e.PRIVATE}/client-state`,host:'omp'});
  const put = (name, value) => p.fs.writeFileString(p.path.join(e.PRIVATE,name),value,
    {mode:0o600,flag:'wx'});
  yield* put('session.token',token);
  yield* put('health.token',token);
  yield* put('broker.json',broker);
  yield* put('plugin.json',plugin);
}).pipe(Effect.provide(Platform.layer)));
JS
```

These configuration examples use existing source schemas/services, not a second configuration model. Earlier stdin/staging checks were run before the semantic cutover; re-exercise the exact current broker, runner and Helm-values examples in disposable fixtures before claiming acceptance. No example in this edited guide has been executed by this documentation task.

Retention values here are explicit one-day archive/three-day sanitized-backup choices, not recovery guarantees; review them for your policy. This intentionally grants only session registration/polling, not task submission. Add separate `coordinator`, `reviewer`, `archive-writer` or `admin` principals only when their operations are needed, with distinct tokens/scopes. Keep acceptance credentials separate from task-control credentials.

### External PostgreSQL URL and CA

Provision dedicated PostgreSQL18 broker and runner databases with separate scoped logins outside the chart. Have your private secret manager inject the selected role's URL as `COORDINATOR_DATABASE_URL` into its service process environment. Paired offline recovery instead receives both broker `COORDINATOR_DATABASE_URL` and optional runner `COORDINATOR_RUNNER_DATABASE_URL`. Do not place URLs in application files, configuration JSON, shell history, command arguments or logs; keep tracing off. Tests require the independent `COORDINATOR_TEST_POSTGRES_URL`, never production credentials.

`DatabaseService.connection()` accepts PostgreSQL URLs with nonempty decoded user/password/database and a host; percent-encode credentials. Remote requires `?sslmode=verify-full`, the only accepted query setting. Fragments, control bytes and extra path components are rejected. Plaintext disable is permitted only for loopback disposable proofs, never remote Docker/cluster databases. Supplied URLs are accepted once as redacted service configuration; no URL reader or config path is involved. Supply trusted roots as `$PRIVATE/ca.crt`; configure the chart CA Secret to trust PostgreSQL and private HTTPS.

When supplied, `NODE_EXTRA_CA_CERTS` must name a valid public CA bundle at a canonical trusted path; invalid/missing/noncanonical trust fails rather than silently falling back. Systemd installation persists that path, remote SSH forwards it as a quoted path, and native children do not receive it. Fully restart owner/client processes after trust rotation; never export CNPG `ca.key`.

The runner URL is required only when enabling a runner and must name its separate database/login. For host schema application/recovery/inspection, set `NODE_EXTRA_CA_CERTS="$PRIVATE/ca.crt"` before startup. Create private host-broker.json/host-runner.json with actual absolute filesystem paths, never Kubernetes-only `/config`; neither JSON contains database credentials or URL paths. Preserve matching identity and stopped pair's backup/repository/model/child roots during supported PostgreSQL recovery. Greenfield initialization uses fresh canonical private state paths. Runtime owns private0700 parents/0600 files without links. Inject only the runner's `COORDINATOR_DATABASE_URL` when starting its normal service; paired recovery's runner variable is not a second normal runtime database.


### Runner extension of this configuration

Before enabling the runner, generate **two independent** 32-byte random hex tokens using the same protected file procedure: runner bearer and broker-to-runner control. SHA-256 hashes use trimmed exact raw token bytes. Add a broker principal with `id` equal to RunnerConfig `id`, your owner, `roles: ["runner"]`, exact repository allowlist, runner bearer `tokenHash`, control `controlTokenHash`, and real private HTTPS `runnerUrl`. Broker and runner ports must match chart values. Populate `repositoryIds` with the actual positive GitHub repository numeric IDs; a checkout name is not a repository ID.

RunnerConfig contains exactly these required fields:

| Field | Required operator value |
| --- | --- |
| `id`, `brokerUrl`, `tokenFile` | Enrolled runner ID, verified private HTTPS broker authority, `/config/runner.token`. |
| `host`, `port`, `controlTokenHash` | `0.0.0.0`, normally `8788`, SHA-256 of the separate control token. |
| `stateDirectory`, `modelDirectory`, `childDirectory` | `/work/runner`, `/work/home`, `/work/children` respectively, or distinct nonoverlapping canonical descendants of `/work`. |
| `slots`, `childUidBase` | Integer 1–64 and reserved UID base at least 1000000; allocate nonoverlapping UID ranges among runners. Configurations above64 must be lowered before upgrading; 64 is an admission bound, not verified 64-child throughput. |
| `repositories` | Object mapping every authorized repository to its distinct canonical `/work/repos/...` checkout root, outside all private/child roots. |
| `launchers` | Object mapping `omp` to `/usr/local/bin/omp`, or `pi` to the actual installed executable in your verified image. |

The capacity heartbeat carries only runner identity, slots and load. Rebuild/reload broker and runners together: the obsolete `journal` heartbeat field is rejected; durable reports and assignment/handoff journal reconciliation remain separate.

`repositories.json` maps **every and only** configured repository to its exact full commit SHA. Init fetches public GitHub repositories via unauthenticated HTTPS and detaches that pin, without running setup scripts. For private/offline repositories, stop the runner and pre-provision a clean exact checkout on the work PVC using a trusted job and separately scoped clone credentials. Init accepts that checkout without fetching, rejects dirt/mismatch and never resets operator data. Neither image source files nor historical Git bundles are task-repository seeds. Changing pins is an explicit stopped-runner provisioning operation.

For the conventional **one-repository OMP runner**, first provision its independent database URL in the secret backend, then export your selected `RUNNER_ID`, verified private `RUNNER_URL`, `TASK_REPO`, exact `TASK_SHA` and positive `TASK_REPO_ID`. This extends broker config before first install; existing installs require maintenance below. Reserve the million-UID base on your host. No database credential is generated by this example.

```sh
export RUNNER_ID RUNNER_URL TASK_REPO TASK_SHA TASK_REPO_ID
node --import tsx --input-type=module <<'JS'
import { Effect, Schema } from 'effect';
import { Hex } from 'effect/encoding';
import { Platform } from './src/platform.ts';
import { BrokerConfig, RunnerConfig, CommandError } from './src/contracts.ts';
const e = process.env;
await Effect.runPromise(Effect.gen(function*() {
  const p = yield* Platform;
  for (const k of ['PRIVATE','OWNER_ID','BROKER_URL','RUNNER_ID','RUNNER_URL','TASK_REPO','TASK_SHA','TASK_REPO_ID'])
    if (!e[k]) return yield* new CommandError({message:`Missing ${k}`});
  if (!/^github\.com\/[a-z0-9_.-]+\/[a-z0-9_.-]+$/.test(e.TASK_REPO) ||
      !/^[a-f0-9]{40}$/.test(e.TASK_SHA) ||
      !Number.isSafeInteger(Number(e.TASK_REPO_ID)) || Number(e.TASK_REPO_ID) <= 0)
    return yield* new CommandError({message:'Invalid repository identity/pin'});
  const broker = Schema.decodeUnknownSync(Schema.fromJsonString(BrokerConfig),
    {onExcessProperty:'error'})(yield* p.privateFile(p.path.join(e.PRIVATE,'broker.json')));
  if (broker.principals.some(principal => principal.id === e.RUNNER_ID))
    return yield* new CommandError({message:'Runner identity already enrolled'});
  const bearer = Hex.encode(yield* p.crypto.randomBytes(32));
  const control = Hex.encode(yield* p.crypto.randomBytes(32));
  const tokenHash = yield* p.digest(bearer), controlTokenHash = yield* p.digest(control);
  const encode = schema => Schema.encodeUnknownSync(Schema.fromJsonString(schema));
  const updated = encode(BrokerConfig)({...broker,
    principals:[...broker.principals,{id:e.RUNNER_ID,owner:e.OWNER_ID,roles:['runner'],
      repos:[e.TASK_REPO],tokenHash,controlTokenHash,runnerUrl:e.RUNNER_URL}],
    repositoryIds:{...broker.repositoryIds,[e.TASK_REPO]:Number(e.TASK_REPO_ID)}});
  const runner = encode(RunnerConfig)({id:e.RUNNER_ID,brokerUrl:e.BROKER_URL,
    tokenFile:'/config/runner.token',
    host:'0.0.0.0',port:8788,controlTokenHash,
    stateDirectory:'/work/runner',slots:1,repositories:{[e.TASK_REPO]:'/work/repos/task'},
    launchers:{omp:'/usr/local/bin/omp'},modelDirectory:'/work/home',
    childDirectory:'/work/children',childUidBase:1000000});
  const put = (name,value) => p.fs.writeFileString(p.path.join(e.PRIVATE,name),value,
    {mode:0o600,flag:'wx'});
  yield* put('runner.token',bearer);
  yield* put('control.token',control);
  yield* put('repositories.json',JSON.stringify({[e.TASK_REPO]:e.TASK_SHA}));
  yield* put('runner.json',runner);
  yield* p.fs.writeFileString(p.path.join(e.PRIVATE,'broker.json'),updated,{mode:0o600});
}).pipe(Effect.provide(Platform.layer)));
JS
```

Prepare the real private `omp.config.yml` and any required `omp.auth.json`/`omp.models.json` separately using the SDK's supported formats. The script deliberately does not guess your model credentials or grant the session principal task-submission authority.

### Existing Secret key contract

| Role | Key | Exact meaning |
| --- | --- | --- |
| Both | `config.json` | Full BrokerConfig or RunnerConfig JSON. |
| Both | `health.token` | Raw 64-lowercase-hex token; broker matches an enrolled principal hash, runner equals `runner.token`; optional trailing whitespace. |
| Both | `database.url` | Native Kubernetes Secret key containing the role-specific PostgreSQL URL; injected directly as `COORDINATOR_DATABASE_URL` through secretKeyRef into runtime and migration init, never staged into application files. Separate broker/runner database/login; remote verify-full only, no other query/fragments/control bytes. |
| Broker with runner | `control.<base64url UTF-8 principal.id without padding>` | Raw control token matching that principal's control hash; staged at `/config/tokens/<lowercase SHA256 of UTF-8 principal.id>` (no extension). The Secret key remains base64url-encoded. |
| Broker with object backup | `backup.access-key`, `backup.secret-key` | Genuine provider credentials; configured destinations must be descendants of `/config`. |
| Runner | `runner.token` | Enrolled runner bearer, **not** the control token. |
| Runner | `repositories.json` | Configured repository → exact full SHA map. |
| Runner with OMP | `omp.config.yml` | JSON ModelProfile: `{"modelRoles":{"default":"your actual SDK model selector"}}`, despite the filename. |
| Runner with Pi | `pi.settings.json` | Actual Pi SDK settings JSON selecting a real accessible model/provider. |
| Runner | `omp.auth.json`, `pi.auth.json` | Genuine selected SDK auth format if provider requires credentials; omit for genuinely no-auth local providers. |
| Runner | `omp.models.json`, `pi.models.json` | Optional genuine SDK custom model/provider definitions. |

The model selector above describes the contract, not a runnable fake model configuration. Supply your real SDK-supported provider/model/auth seed and prove a real model turn before admitting work. Init validates JSON/staging, not successful provider semantics. Seed only model settings/auth/definitions: no histories, sessions, prompts, plugin enrollment credentials, arbitrary HOME files or symlinks. Removed known optional seed files are removed from staged storage so rotation cannot silently reuse old auth.

Compute a control-key name without exposing its value:

```sh
CONTROL_KEY="control.$(node -p 'Buffer.from(process.argv[1]).toString("base64url")' "$RUNNER_ID")"
```

## 4. Provision namespace, Secrets, storage and TLS

Choose/export `NAMESPACE`, `RELEASE`, `BROKER_SECRET`, `OPERATOR_VALUES`, and optional `RUNNER_SECRET`. Obtain `IMAGE_REPOSITORY`, `IMAGE_DIGEST`, `SOURCE_SHA` from the **same verified release manifest**; never substitute an old research image. The following commands mutate your selected cluster and are only explicit operator installation steps:

```sh
set -o pipefail
env -u COORDINATOR_DATABASE_URL -u COORDINATOR_RUNNER_DATABASE_URL -u COORDINATOR_TEST_POSTGRES_URL \
  kubectl create namespace "$NAMESPACE"
env -u COORDINATOR_DATABASE_URL -u COORDINATOR_RUNNER_DATABASE_URL -u COORDINATOR_TEST_POSTGRES_URL \
  kubectl -n "$NAMESPACE" create secret generic "$BROKER_SECRET" \
  --from-file=config.json="$PRIVATE/broker.json" \
  --from-file=health.token="$PRIVATE/health.token" --dry-run=client -o json |
  node --input-type=module -e '
    let input=""; for await (const chunk of process.stdin) input+=chunk;
    const secret=JSON.parse(input), url=process.env.COORDINATOR_DATABASE_URL;
    if (!url) throw new Error("Missing COORDINATOR_DATABASE_URL");
    secret.data["database.url"]=Buffer.from(url).toString("base64");
    process.stdout.write(JSON.stringify(secret));
  ' | env -u COORDINATOR_DATABASE_URL -u COORDINATOR_RUNNER_DATABASE_URL -u COORDINATOR_TEST_POSTGRES_URL \
    kubectl -n "$NAMESPACE" create -f -
```

Run this pipeline only in the trusted secret-manager environment with the broker URL injected; its Secret JSON goes directly to kubectl stdin, never to a saved manifest or log. Enable shell `pipefail` so a failed stage fails the command. For runners add the exact broker control-key entry `--from-file="$CONTROL_KEY=$PRIVATE/control.token"` to the initial kubectl command. For object backup add both backup credential files and configure the HTTPS provider endpoint/region/bucket and private `/config` destinations. File-based token arguments expose paths/key names, never raw token bytes. Prefer your existing secret reconciler to populate `database.url` and other keys for steady-state provisioning/rotation; provider CRDs are not required. Do not write Secret YAML/JSON containing plaintext/base64 secrets to deployment Git or CI artifacts.

For the one-repository OMP runner above, create its Secret with the required files explicitly:

```sh
set -o pipefail
# Inject the runner's independent URL as COORDINATOR_DATABASE_URL for this command.
env -u COORDINATOR_DATABASE_URL -u COORDINATOR_RUNNER_DATABASE_URL -u COORDINATOR_TEST_POSTGRES_URL \
  kubectl -n "$NAMESPACE" create secret generic "$RUNNER_SECRET" \
  --from-file=config.json="$PRIVATE/runner.json" \
  --from-file=runner.token="$PRIVATE/runner.token" \
  --from-file=health.token="$PRIVATE/runner.token" \
  --from-file=repositories.json="$PRIVATE/repositories.json" \
  --from-file=omp.config.yml="$PRIVATE/omp.config.yml" --dry-run=client -o json |
  node --input-type=module -e '
    let input=""; for await (const chunk of process.stdin) input+=chunk;
    const secret=JSON.parse(input), url=process.env.COORDINATOR_DATABASE_URL;
    if (!url) throw new Error("Missing COORDINATOR_DATABASE_URL");
    secret.data["database.url"]=Buffer.from(url).toString("base64");
    process.stdout.write(JSON.stringify(secret));
  ' | env -u COORDINATOR_DATABASE_URL -u COORDINATOR_RUNNER_DATABASE_URL -u COORDINATOR_TEST_POSTGRES_URL \
    kubectl -n "$NAMESPACE" create -f -
```

Append `--from-file=omp.auth.json="$PRIVATE/omp.auth.json"` and/or `--from-file=omp.models.json="$PRIVATE/omp.models.json"` with actual private files when the selected provider requires them; omission is correct only for genuine no-auth/default-definition providers. The broker Secret must contain the added control entry and updated broker config **before** the runner starts. If you already installed broker-only, replace that Secret through the controlled stopped-service procedure, not a second `create` against an existing name.

Use chart values containing **references only**, for example generate broker-only values from exported verified coordinates:

```sh
export IMAGE_REPOSITORY IMAGE_DIGEST SOURCE_SHA BROKER_SECRET OPERATOR_VALUES
node --import tsx --input-type=module <<'JS'
import { Effect } from 'effect';
import { Platform } from './src/platform.ts';
import { CommandError } from './src/contracts.ts';
const e = process.env;
await Effect.runPromise(Effect.gen(function*() {
  const p = yield* Platform;
  for (const k of ['IMAGE_REPOSITORY','IMAGE_DIGEST','SOURCE_SHA','BROKER_SECRET','OPERATOR_VALUES'])
    if (!e[k]) return yield* new CommandError({message:`Missing ${k}`});
  yield* p.fs.writeFileString(e.OPERATOR_VALUES,JSON.stringify({
    image:{repository:e.IMAGE_REPOSITORY,digest:e.IMAGE_DIGEST},sourceSha:e.SOURCE_SHA,
    broker:{existingSecret:e.BROKER_SECRET},runner:{enabled:false}
  },null,2),{mode:0o600,flag:'wx'});
}).pipe(Effect.provide(Platform.layer)));
JS
```

JSON is accepted as Helm YAML. For runner installation add `runner.enabled: true` and `runner.existingSecret`; set ports matching configs, resources, optional TLS hostname→socket-DNS `connectHosts`, and `extraEnv` Secret references. `connectHosts` does not disable hostname/SNI/certificate validation. GitHub evidence reads need a separately scoped token Secret referenced by the exact `githubTokenEnv` environment name; neither broker GitHub token nor model auth is implicitly shared.

`tls.existingCaSecret` references an existing Secret containing `ca.crt` (PEM trust roots, no private key). It mounts read-only and sets `NODE_EXTRA_CA_CERTS` for migration, broker and enabled runner. The shared bundle must trust the remote PostgreSQL certificate and private HTTPS authorities. Remote PostgreSQL always uses verify-full, including hostname verification; URL query contains only sslmode, with CA trust supplied separately. Leave the CA reference empty only for already trusted issuers. Native dump/restore maps the same bundle to `PGSSLROOTCERT`; no verification bypass is supported.

Create the trust Secret from the reviewed public bundle, then persist its reference in OPERATOR_VALUES (never private CA keys):

```sh
kubectl -n "$NAMESPACE" create secret generic "$CA_SECRET" \
  --from-file=ca.crt="$PRIVATE/ca.crt"
```

Set `tls.existingCaSecret` before install; extend default-deny policy to the exact PostgreSQL destination/port for broker and runner migration init and runtime. Mac access needs its pinned private endpoint/host ACL and trusted native CNPG public CA, not wildcard arbitrary-device SQL grants.


Each role exposes `broker.deploymentAnnotations`, `broker.podAnnotations`, `runner.deploymentAnnotations` and `runner.podAnnotations`, all defaulting to `{}` and accepting string maps. Deployment annotations target the Deployment; pod annotations target its pod template. Put operator-owned Secret-reloader or Velero-exclusion annotations at the scope required by your installed controller in the downstream overlay. No provider controller is installed by these maps, and annotations do not replace fenced recovery or the controlled restart procedure.

Default PVCs are `${RELEASE}-backups` (10Gi) and `-work` (20Gi, runner only), RWO. `persistence.backups|work` accepts existingClaim/storageClass/size/accessModes; null storageClass uses cluster default, empty selects no provisioner; RWOP supported. Work PVC retains repositories/model/children/agent files and private runner.lock; SQL journals live in the runner's independent PostgreSQL database. Chart keep/Retain/RWO are not backups; protect both external databases and work files independently.

Initializer commands are exactly:

```sh
node --import tsx scripts/deploy-init.ts broker --input /input --config /config
node --import tsx scripts/deploy-init.ts runner --input /input --config /config
```

Root staging has only CHOWN/FOWNER/DAC_OVERRIDE. Secret input is read-only0400; config is memory-backed. Each role's `database.url` Secret key feeds `COORDINATOR_DATABASE_URL` directly through secretKeyRef into runtime and migration init, not a staged application file or JSON field. Broker backupDirectory is at/below `/backups`. Each role's migration init runs `scripts/migrate.ts` on the same immutable image UID1000/cap0 with read-only config/CA and writable tmp. Failure blocks startup; Recreate stops old owner before migration. No skip/live-owner PreSync. Generic product chart **`suspended: true`** holds both Deployments at zero; maintainer operator wrapper **`runtime.suspended: true`** maps to it and disables its acceptance hook. Keep suspension/reconciliation controls until fenced offline migration completes; then perform controlled activation and acceptance.

Runtime root/config filesystems are read-only, tmp writable, service-account automount disabled. Broker runs UID1000/capability0. Runner setpriv becomes UID1000 with exactly CHOWN/DAC_OVERRIDE/KILL/SETUID/SETGID/SETPCAP, **no FOWNER**; children have distinct UIDs, zero capabilities/groups and NoNewPrivs. Do not weaken isolation for admission.

### Local Docker: stage → migrate → broker

Use the same verified image digest, external reachable TLS PostgreSQL18, and private inputs. Prepare `$PRIVATE/docker-input`0700 containing config.json (cluster broker.json) and health.token0600, without database URLs. Have your secret manager inject the broker's exported `COORDINATOR_DATABASE_URL`; Docker forwards it by name, never as a literal argument. Export distinct CONFIG_VOLUME, BACKUPS_VOLUME, BROKER_CONTAINER names and CA_DIRECTORY with only trusted ca.crt. These commands mutate the operator's Docker host:

```sh
IMAGE="$IMAGE_REPOSITORY@$IMAGE_DIGEST"
docker volume create "$CONFIG_VOLUME"
docker volume create "$BACKUPS_VOLUME"
docker run --rm --user 0:0 --cap-drop ALL \
  --cap-add CHOWN --cap-add FOWNER --cap-add DAC_OVERRIDE \
  --security-opt no-new-privileges --read-only --tmpfs /tmp \
  --mount "type=bind,src=$PRIVATE/docker-input,dst=/input,readonly" \
  --mount "type=volume,src=$CONFIG_VOLUME,dst=/config" \
  --mount "type=volume,src=$BACKUPS_VOLUME,dst=/backups" \
  "$IMAGE" node --import tsx scripts/deploy-init.ts broker --input /input --config /config
docker run --rm --user 1000:1000 --cap-drop ALL \
  --security-opt no-new-privileges --read-only --tmpfs /tmp \
  --mount "type=volume,src=$CONFIG_VOLUME,dst=/config,readonly" \
  --mount "type=bind,src=$CA_DIRECTORY,dst=/etc/coordinator/ca,readonly" \
  --env COORDINATOR_DATABASE_URL \
  --env HOME=/tmp \
  --env NODE_EXTRA_CA_CERTS=/etc/coordinator/ca/ca.crt \
  "$IMAGE" node --import tsx scripts/migrate.ts
# Continue only after both commands exit zero.
docker run --detach --name "$BROKER_CONTAINER" --user 1000:1000 --cap-drop ALL \
  --security-opt no-new-privileges --read-only --tmpfs /tmp \
  --publish 127.0.0.1:8787:8787 \
  --mount "type=volume,src=$CONFIG_VOLUME,dst=/config,readonly" \
  --mount "type=volume,src=$BACKUPS_VOLUME,dst=/backups" \
  --mount "type=bind,src=$CA_DIRECTORY,dst=/etc/coordinator/ca,readonly" \
  --env COORDINATOR_CONFIG=/config/broker.json \
  --env COORDINATOR_DATABASE_URL \
  --env COORDINATOR_RUNNER_TOKEN_DIRECTORY=/config/tokens \
  --env "COORDINATOR_IMAGE_SHA=$SOURCE_SHA" \
  --env NODE_EXTRA_CA_CERTS=/etc/coordinator/ca/ca.crt \
  "$IMAGE"
```

Stop/fence the old container before staging/migrating upgrades. Expose loopback through an operator-owned verified private TLS proxy. Docker has no enforced init dependency: stop on any failure. Use a fresh private config volume for rotation. Runner deployment still requires six-capability setpriv/work preparation/genuine SDK seeds/independent child isolation; follow chart entrypoints rather than privileged containers.


Services `${RELEASE}-broker` and `${RELEASE}-runner` are **internal HTTP ClusterIP**. Operator/provider-owned TLS termination must provide private HTTPS matching `brokerUrl`/`runnerUrl` before enabling the runner; serving certificates, renewal, private keys, DNS and routes remain outside the chart. Configure `tls.existingCaSecret` for private issuers as above; never set insecure verification flags. Keep the HTTPS URL authority as the certificate's real hostname. `connectHosts` may change socket DNS routing, not the URL authority, hostname validation or SNI. Broker-only can start without any host route, but external acceptance requires the real private route.

Create default-deny policies using labels `app.kubernetes.io/name=coordinator`, `app.kubernetes.io/instance=<release>`, `app.kubernetes.io/component=broker|runner`. Explicitly allow DNS, kubelet probes, trusted TLS gateways/clients, runner→broker, broker→runner control, required GitHub fetch/evidence, optional object backup, and selected model/auth endpoints. Prevent native children from obtaining broker/control credentials. TLS gateway/provider policy and child isolation require actual operator tests; a rendered chart does not enforce an unspecified network.

## 5. Disposable install and GitOps promotion

First render/review and run a disposable install using `CHART` as the verified `coordinator-0.2.0-beta.5.tgz` after publication, or exact-source chart directory for local preparation. Do not invent public artifacts or borrow earlier release pins.

```sh
helm lint "$CHART" -f "$OPERATOR_VALUES"
helm template "$RELEASE" "$CHART" -n "$NAMESPACE" -f "$OPERATOR_VALUES" > "$RENDERED_MANIFEST"
helm upgrade --install "$RELEASE" "$CHART" -n "$NAMESPACE" -f "$OPERATOR_VALUES" --wait --timeout 10m
kubectl -n "$NAMESPACE" rollout status "deployment/$RELEASE-broker" --timeout=120s
node --import tsx scripts/deployment-smoke.ts \
  --config "$PRIVATE/plugin.json" --source-sha "$SOURCE_SHA" --cycles 5
```

`--cycles` accepts integers **1–100**, default **5**, with at most five concurrent heartbeat/poll cycles. Record the real successful counts, peak in-flight work, completions while concurrent, elapsed time and per-operation latency output; configured concurrency alone is not evidence. This bounded no-dispatch probe does not promise sustained capacity or replace slow-SSE first-event/abort/concurrent-client acceptance.

After the stopped-service broker config/control-key update and all runner prerequisites, persist `runner.enabled: true` and `runner.existingSecret` in the reviewed operator values. For an explicit disposable runner install the equivalent command is:

```sh
helm upgrade --install "$RELEASE" "$CHART" -n "$NAMESPACE" \
  -f "$OPERATOR_VALUES" --set runner.enabled=true --set-string "runner.existingSecret=$RUNNER_SECRET" \
  --wait --timeout 10m
kubectl -n "$NAMESPACE" rollout status "deployment/$RELEASE-runner" --timeout=120s
node --import tsx scripts/deployment-smoke.ts \
  --config "$PRIVATE/plugin.json" --source-sha "$SOURCE_SHA" --cycles 5
```

The command-line override is for the disposable gate, not a substitute for persisting those desired values in GitOps. Do not enable the runner solely to satisfy rendering when its real TLS, provider seed and pinned repository inputs are missing.

Set `RENDERED_MANIFEST` to a private review file first. `deployment-smoke` requires private absolute PluginConfig/token/state paths, an existing `session` principal and verified HTTPS (HTTP permitted only disposable loopback). It uses the real stateless MCP client and TLS-preserving connectHosts. It verifies missing/invalid auth rejection, `/readyz` status/body/full source header, MCP health, DB-backed isolated heartbeat/polls, and scoped session stop. Unique `.invalid` repository and no worker ID prevent work offers/dispatch; no runner/task/model call is made. All calls have a combined 60s deadline, cleanup a separate 10s, redacted bounded JSON and nonzero failure. It writes a temporary session: run only with explicit operator acceptance authorization.

When that route uses a private CA, launch host acceptance with the same trusted public CA bundle: `NODE_EXTRA_CA_CERTS="$PRIVATE/ca.crt" node --import tsx scripts/deployment-smoke.ts --config "$PRIVATE/plugin.json" --source-sha "$SOURCE_SHA" --cycles 5`. Obtain the bundle through the operator's trusted channel; the host environment does not inherit Kubernetes mounts. Set `NODE_EXTRA_CA_CERTS` before starting ordinary `omp`/`pi` clients too, and fully restart them when trust changes. Node loads additional roots at process startup. Do not replace the real HTTPS authority with an IP/internal Service name merely to bypass routing; preserve certificate hostname/SNI verification.

Broker `/livez` is cheap unauthenticated process liveness; authenticated `/readyz` checks database ownership/SELECT1 and reports `x-coordinator-source-sha`. PostgreSQL ownership loss stops database serving/writes/readiness (503), not cheap liveness (200), so admitted native work can survive until validated completion. Do not use readiness failure as an automatic destructive restart: drain explicitly, then restart without PostgreSQL reconnect/replay. The local kernel ownership fence remains hard; no role's authenticated health becomes public.

Before routing real work, additionally prove runner readiness, clean pinned public/private checkout behavior, a real selected SDK/provider model turn, child credential/UID isolation, broker lock fencing, durable restart/no-replay and protected recovery rehearsal. Do not narrow those to mocks or call successful no-dispatch smoke full runner acceptance.

After disposable acceptance, submit an operator deployment PR pinning the exact verified chart artifact/checksum or published OCI digest, image digest and source SHA together. Include producer/source verification evidence, rendered ordinary APIs, Secret references, PVC retention/ownership, TLS/policy flows, maintenance plan and recorded acceptance. Deployment CI/admission enforce these independently; Argo owns reconciliation after reviewed merge. Source CI cannot certify a foreign operator cluster and this product workflow does not push operator Git, dispatch production workflows or apply the PR. Argo Healthy/Synced is controller state; run the real private-route no-dispatch smoke after sync and record failure rather than silently accepting.

## 6. Client upgrades and maintenance

Install the exact-source client on each Pi/OMP host with `pnpm install --frozen-lockfile && pnpm build`, then nonroot `omp plugin link /absolute/coordinator` or `pi install /absolute/coordinator`. Use a clean distribution containing exactly `dist/agent/{protocol,client,server}.{js,d.ts}` and `dist/plugins/{plugin,agent-session}.js`; no separate package installation is needed. Ordinary hosts load only plugin.js; isolated runner children load only agent-session.js. Remove obsolete emitted modules/bundles when rebuilding, migrate installed deep imports to the matching three-module API, and fully restart/reload matching clients and servers before activation; no old-path aliases exist. PluginConfig defaults to `$XDG_CONFIG_HOME/coordinator/plugin.json` or `~/.config/coordinator/plugin.json`; file/token0600, parent0700. `node --import tsx scripts/install.ts --help` describes `--plugin-only --plugin-config <file>`. Linux root `--runner-only` is a separate isolated service install, not a Kubernetes prerequisite. Never replace global SDK binaries/profiles or copy a maintainer HOME.

The selected source layout is13 handwritten +5 generated files; the [consolidation report](src-consolidation-plan.md) records every owner, callable seam and layer. DatabaseService lives in database.ts; broker.ts owns dispatch/GitHub factories, plugin.ts owns concrete Journal/status factories, and backup.ts exports makeSnapshotStore directly. Six application Context owners remain; removed configuration/Journal/snapshot/inert owner tags are not compatibility APIs. Nine program descriptors live at their existing main/script owners while66 shared root descriptors include the two added local participation commands. These moves do not attach to the human's native conversation: pinned void native message APIs lack durable per-message admission. ModelProfile and native profile seeds remain real dependencies. Rex's Linux artifact/compatibility remains unproven; Hermes is optional as a client, not a new service, board or automatic deployment path.

Use `/coordinator off` to stop this host's new coordination and sharing without aborting accepted agent work. The private, atomic/fsynced `stateDirectory/off.json` preference survives restart; no SDK owner/endpoint is created while disabled. Archive metadata is revoked/deleted, local archive spool erased, and delivery/no-replay state retained. If remote cleanup is unconfirmed, restore connectivity and retry `off` explicitly; `on` refuses pending archive cleanup. `/coordinator on` resumes participation but never grants archive consent, and pre-off accepted outcomes still require explicit reconciliation. `/coordinator optout` remains archive-only. Other live hosts must opt out independently; this is not a broker/runner shutdown.

Client/operator JSON uses Effect Formatter's two-space JSON presentation; MCP successes and sanitized failures carry both readable text and structured data. Strict Schema codecs keep wire/storage/canonical data compact. Root broker/runner successful response envelopes are bounded at192KiB to accommodate duplicated/escaped presentation, while requests and rejected HTTP bodies stay64KiB and discovery512KiB. Original structured payload quotas remain enforced separately; standalone private-agent bounds and large-result resource handles are unchanged.

For the host runner installer, have the secret manager inject that runner's `COORDINATOR_DATABASE_URL` before `--runner-only`. The installer validates it and provisions the native systemd EnvironmentFile `/etc/coordinator-runner/environment`, root-owned0600 under a root-only0700 parent; the unit references only the path. Systemd supplies the persistent service environment across reboot. This is native supervisor provisioning, not a URL path in RunnerConfig or an application file reader. Keep the environment file private, provision a replacement through the controlled installer/secret-manager process when credentials rotate, and restart under the same drain/fence controls. Remote installation requires secret provisioning on the remote host; SSH arguments never carry database credentials.

**Complete all-client cutover:** stop new submissions and approved releases at every client, suspend reconciliation/acceptance and reconcile actual tasks/children before draining/stopping owners. Rebuild every CLI, plugin, runner extension and compiled-module consumer. Replace owned wire methods with `agent.*`, resource URIs with `agent://`, metadata with `io.coordinator.agent/*`, bindings/cursors with `agentBinding`/`agentRunId`/`agentCursor`, and explicit owner config with `COORDINATOR_AGENT_CONFIG`/`AgentConfig`. Owner files are `agent.token`, `agent-config.json`, `agent-endpoint.json`; plugin state is `agent-<digest>`. There are no old keys/names/aliases or automatic state rewrites. SDK-owned methods/events remain upstream names. Fully restart/reload all clients and matching servers under maintenance; existing TUIs retain loaded code after linking/building. Initialize fresh separate PostgreSQL databases with the one initial schema, start broker then runner/fresh clients, prove exact source/protocol/binding/TLS/auth/no-replay/no-dispatch acceptance, then explicitly reopen work. Do not let old and new clients coexist or infer activation from a successful build.

For every change, stop task submission/approved release at all coordinator clients and restrict submission-capable ingress/principals as necessary. There is **no dedicated maintenance-mode/drain endpoint**. A quiet heartbeat or empty no-dispatch smoke is not proof of no work. Inspect known tasks with authorized `task.get`/`task.attach`, reconcile runner state and native children, then stop runner and broker before offline state operations. Retain uncertain/occupied HELD rows; do not clear capacity by editing the database.

Each separate PostgreSQL database has exactly one runtime/migration/recovery session-advisory owner. Duplicates fail closed; PG owner loss permanently revokes database serving/writes/readiness, with no automatic reconnect/replay, while admitted native work survives to validated completion and cheap liveness stays200. Drain explicitly before restart; retain uncertain accepted work HELD. Locks do not fence copied databases. Recreate stops old owners before migration; no HA/replica scaling. Runner filesystem kernel flock remains a hard fence. Reconcile native children independently of pod deletion.

For GitOps reconcile namespace/private Secrets/CA/external database connectivity first, then the digest-pinned Recreate broker Deployment. Await its ordinary migration init/readiness before enabling runner or submission routes. No PreSync migration hook against a live owner. Suspend self-heal before offline recovery; premature desired-replica restoration is unsafe. Secret reload controllers must follow controlled downtime.
For maintenance, suspend operator reconciliation first (especially Argo self-heal), record desired pins and storage ownership, then explicitly stop workloads. Imperative scale-to-zero alone is not durable under reconciliation:

```sh
kubectl -n "$NAMESPACE" scale "deployment/$RELEASE-runner" --replicas=0
kubectl -n "$NAMESPACE" scale "deployment/$RELEASE-broker" --replicas=0
```

Skip the runner command for broker-only. Wait for all source pods/native children to stop and independently fence old volumes/endpoints before offline backup/restore; these scale commands themselves are not the proof. Controlled restart uses reviewed desired replicas/image/config, broker first then runner, followed by private-route acceptance. The chart defines one replica per enabled role; scaling is an explicit temporary operator exception with reconciliation suspended.

### Secret and model rotation

Stop new work and reconcile occupied work first. Generate distinct replacement tokens privately, update raw Secret keys and matching hashes in both authoritative configs together (runner bearer, health bearer, control token/hash as applicable). Revoke old session credentials in broker config and rotate provider credentials in the genuine SDK format. Preserve identity/repository scope unless intentionally re-enrolling. Use your secret controller or file-based replacement without printing decoded values.

Chart checksum tracks **values**, not Secret bytes. Running services keep loaded configs and staged `/config`; Kubernetes Secret refresh alone does not rotate them. With services controlled/stopped, update Secrets, then recreate pods so init restages UID1000/0600 files and removes absent known model-auth seeds. For explicit operator restart use `kubectl -n "$NAMESPACE" rollout restart "deployment/$RELEASE-broker"` and, if enabled, the runner equivalent in controlled order; coordinate any downstream reloader so it cannot create an unreviewed mid-work restart. Reload native clients using their new private token files, prove new credentials and old-credential rejection, then reopen submissions. Do not log token/hash-bearing config or assume overlap/revocation semantics that are not configured.

For CA rotation, stage a reviewed overlap bundle containing old and new trusted roots in the shared `ca.crt` Secret and on client/acceptance hosts. Under the same maintenance controls, restart broker then runner and all Node clients so the new roots are loaded; coordinate annotation-driven reloaders rather than allowing surprise mid-work restarts. The provider separately rotates serving certificates/routes. Prove both authenticated private routes with normal authority/SNI checks, then remove the retired root when no required endpoint depends on it and restart/recheck again. A projected Secret refresh is not evidence that a running Node process reloaded trust; retain HELD/no-replay semantics throughout.

## 7. Inspect no-work and occupied HELD state

There is no all-task maintenance CLI. Use authorized task.get/task.attach for known IDs while serving. After services/children are stopped and fenced, inspect only bounded metadata through a private local operator process, never credentials in arguments or a full database dump:

Inspect metadata through the existing scoped `DatabaseService` from `src/database.ts` and its generated-type `query` API; do not construct a second driver/pool or use retired database interfaces. Inject the selected role's private `COORDINATOR_DATABASE_URL` and trusted CA before opening that offline owner. Use typed Kysely selections of task state and runner task ID/state/occupied/PID, without dumping payloads or credentials. PIDs do not prove outcome: inspect actual owned processes/workspaces/effects. QUEUED/ASSIGNING/ACCEPTED/RUNNING or occupied rows prevent idle claims; HELD remains uncertain. Preserve fingerprints/incarnations and never force-free/reissue by editing SQL.

## 8. Protected backup and fenced restore

### Application exports versus database disaster recovery

The existing online **application pg_dump snapshots** remove archive/consent/participant-consent rows before local/object export. They still contain ledger/task/evidence/session/queue information and require protected access; “sanitized” does not mean safe to publish. Object storage is optional and operator-owned. `backup-smoke.ts` exercises the sanitized object-store primitive, **not** full runner recovery. Downloading a sanitized snapshot to a new0600 file is not replacing/restoring a production ledger.

CNPG/Barman **physical base backups and WAL/PITR are a separate operator-owned product**, not these archive-sanitized application exports or the paired recovery artifact below. A database restore does not reconcile runner files or fence uncertain accepted work; use the stopped-owner/paired recovery procedure for that boundary. The beta backend's scoped backup/read/restore proof remains required independently of local application recovery.

Protected recovery contains full private broker/consent/task/session data, paired PostgreSQL runner journal and repository/agent files. **v3 recovery.json** binds source/schema/counts/runner layout/hashes/UID/GID/modes/internal links; files are **broker.pgdump**, optional **runner.pgdump** and blobs/<decimal index>. Prisma schema metadata and recovery_gate are excluded; apply the same committed initial schema to each fresh target first. Artifacts are private0600/0700, not automatically encrypted; independently escrow excluded credentials.

Operator configs/enrollment/control tokens/model seed and known copied child model-auth/config/agent token/plugin/endpoint files are excluded. External host plugin journals/history are operator-owned and excluded: no resumed peer session or agent reattachment is promised. Other application files can contain secrets: treat every byte as confidential. Reconstruct excluded credentials/model seed independently from the private secret backend; keep config files outside archived repository/child roots.

### Backup command

Suspend reconciliation; close submissions; stop broker, runner and native descendants; prove old ownership and effects are fenced. The command never stops services for you. Use the **same exact release source** and installed dependencies. Recovery file/config/artifact paths are canonical absolute paths with private owner0700 parents; configuration files0600 owned by the invoking user. The only narrow exception is the runner state directory: its leaf must be0700 and its operator-owned immediate parent may be0700 or0711, matching `/work` traversal;0751 is rejected. Artifact/config parents remain strict private0700. Ancestors cannot be unsafe/symlinked. The artifact directory must not already exist. Runner files may need root access to preserve distinct child UID ownership; run in a controlled offline admin environment with the same volume paths, not a generic network-facing service. Database URLs are service environment secrets, not filesystem preconditions.

Recovery filesystem operations and bounded file streams use the existing Effect Platform and scoped handles; durable writes still fsync files and their parent directories. The pinned Effect API has no `lstat`/`lchown` or incremental digest, so recovery invokes fixed Linux coreutils through Effect ChildProcess: `/usr/bin/stat` for non-following metadata, `/usr/bin/chown --no-dereference` for link ownership, and `/usr/bin/sha256sum` with streamed stdin. These binaries are required alongside `flock`/`setpriv`; no new JavaScript dependency or runner FOWNER capability is needed.

Inject the source broker URL as `COORDINATOR_DATABASE_URL` and, for a paired backup, the distinct source runner URL as `COORDINATOR_RUNNER_DATABASE_URL` through the private secret manager. Restore requires the corresponding fresh target URLs instead. The helper acquires separate scoped database owners and runner filesystem fencing; it never shares the broker connection with the runner. Native pg_dump/pg_restore use the exact owning service's redacted connection through scoped private0600 pgpass, which is tooling authentication, not an application URL file. Database environment secrets must not reach native agent/model/provider children, Git or irrelevant subprocesses and are excluded from recovery artifacts.

```sh
node --import tsx scripts/recover.ts backup \
  --broker-config "$PRIVATE/host-broker.json" \
  --runner-config "$PRIVATE/host-runner.json" \
  --artifact "$RECOVERY_ARTIFACT" --source-sha "$SOURCE_SHA" \
  --offline-maintenance
```

Omit runner config only for genuine broker-only topology; nonterminal assigned work requires its matching runner. Multi-runner recovery is unsupported by this single-runner command. Helpers acquire both PostgreSQL owners plus runner.lock, reject live/unknown children/unsafe paths and stateDirectory files other than runner.lock. They never stop services/delete source/replay work. Native clients use scoped private0600 pgpass, never credential argv.

### Restore command and explicit accepted-work hold

1. Keep source services/children offline and independently fence old endpoints/databases/volumes, including copies on other machines. An advisory lock cannot fence another copied database.
2. Provision separate fresh empty PostgreSQL broker/runner databases and fresh runner volumes at identical absolute paths/identity/UID base, retaining old state. Reconstruct private target configs, URLs/model credentials. Apply the exact committed initial schema separately before restore; runner stateDirectory may contain only runner.lock.
3. Recover the protected artifact securely. Recovery durably sets recovery_gate false before mutation and true only after the paired operation completes. Any partial failure stays fenced: quarantine and retry into separately fresh targets, never bypass the gate.
4. Inspect protected manifest `broker.pendingAccepted` and optional `runner.pendingAccepted` offline. Broker ACCEPTED/RUNNING tasks could redeliver peer release and pending occupied runner ACCEPTED rows would ordinarily auto-launch. Restore requires explicit **`--hold-accepted-work`** acknowledgment when either count is nonzero; it parks copied broker ACCEPTED/RUNNING tasks and pending runner ACCEPTED rows HELD before startup, preserving acceptance/release IDs, fingerprints, work/incarnations and offer decisions. This is not consent to execute them, and external host journals remain excluded.

Apply the exact source package to each fresh target separately. Have the secret manager inject that target's URL as `COORDINATOR_DATABASE_URL` for each migration process; run this once for the broker target and once for the runner target (omit the latter for broker-only):

```sh
NODE_EXTRA_CA_CERTS="$PRIVATE/ca.crt" node --import tsx scripts/migrate.ts
```

Then inject the target broker `COORDINATOR_DATABASE_URL` and paired runner `COORDINATOR_RUNNER_DATABASE_URL` into the restore process:


```sh
node --import tsx scripts/recover.ts restore \
  --broker-config "$PRIVATE/host-broker.json" \
  --runner-config "$PRIVATE/host-runner.json" \
  --artifact "$RECOVERY_ARTIFACT" --source-sha "$SOURCE_SHA" \
  --offline-maintenance --hold-accepted-work
```

Omit runner config for a broker-only artifact. The hold flag is valid for restore only. Matching both databases/manifest/file hashes/layout is validated before copying; execution is not globally transactional across every filesystem file. On **any** failure keep partial fresh volumes offline/quarantined; do not start them or automatically clean/delete/roll back. Diagnose privately and retry only into a separately prepared fresh target. A zero exit means restore completed, not that unknown external effects were undone.

5. Inspect copied HELD state offline and the returned total/broker/runner fenced-row counts. Start the matching broker first, then runner, with reconstructed private credentials/model seed and controlled ingress. Let existing reporting/reconciliation preserve uncertainty; never reissue work to force agreement. Verify readiness, exact source and no-dispatch acceptance.
6. Inspect HELD tasks/native outcomes, archive/session consent and capacity before reopening submissions. Restored sessions require fresh heartbeat and do not establish live peer ownership. Retain unknown results and occupied capacity when physical child/outcome proof is absent. There is no generic safe auto-replay or arbitrary stdio reattachment.

### Disposable recovery rehearsal

The implemented local production Broker/Runner rehearsal is:

```sh
node --import tsx scripts/recovery-smoke.ts
```

The disposable fixture's scratch filesystem must permit execution of its local launcher-version script. Docker `--tmpfs` defaults to `noexec`; use `exec,nosuid,nodev` for that isolated scratch mount rather than weakening the runner's UID/capability proof.

Run only on a disposable Linux amd64 host/container with Node24, PostgreSQL18 tooling/fresh fixture databases, independent `COORDINATOR_TEST_POSTGRES_URL`, dependencies, root/six supervisor capabilities, flock/setpriv and recovery coreutils, no production mounts/credentials. The rehearsal uses real broker/runner services and a local fixture launcher, not a model provider. Run the renamed gate after integration; historical results do not certify current source.

Image reverts never downgrade/mend data or undo external effects. Incompatible schemas require reviewed migration/recovery rehearsal; same-source pairing is not arbitrary N−1 migration or HA. Accept downtime rather than replay.

## 9. Evidence required before release/promotion

Record exact source SHA, successful trusted-source CI run, image digest/signature/provenance/SBOM, chart version and archive checksum (OCI digest only after actual publication), supported client/SDK versions and freshly reloaded client identity. Separately record disposable clean install, private TLS/auth rejection/SELECT1/no-dispatch evidence, runner independent pinned checkouts/real provider model turn/isolation, duplicate broker fencing, durable restart/no-replay, protected recovery rehearsal and actual off-cluster retrieval/secret reconstruction. Do not print raw config/tokens/DB contents or publicize full recovery metadata.

The semantic cutover remains prepared, unpublished and not live. Pre-rename records report122/122 tests across18 files with zero skips, stock Pi/OMP smoke56 fixture calls/zero external, poll503 heartbeat/poll recovery, presence403 pause and zero retired traffic. Earlier paired PostgreSQL recovery retained work files/occupied uncertainty, excluded credentials and launched nothing; ownership-loss proof recorded admitted completion with ready503/live200 and restart HELD/no replay. These are historical observations, not renamed-source or operator acceptance.

Outstanding prerequisites are concrete: authorized private artifacts/source access; completed source/history/redistribution review for any public release; owner-controlled source/deployment branch protection where platform permits; cluster admission trust configuration; operator Secret/TLS/DNS/model/repository/storage inputs; and recorded main/operator gates. None is supplied by a fake sample credential, `.invalid` model, historical homelab Healthy status or unexecuted command.

Release-owner validation commands below are requirements, not executed by this documentation update. Use disposable PostgreSQL18, private-HOME and the real Linux UID1000/six-capability/no-FOWNER/NoNewPrivs boundary; stock host smoke additionally needs pinned actual CLI paths/version environment. Retain the entire suite and production-image migration/startup gates:

```sh
pnpm install --frozen-lockfile
pnpm build
EXPECTED="$(mktemp -d)"
cp -a src/generated/. "$EXPECTED/"
pnpm check
diff --recursive --unified "$EXPECTED" src/generated
rm -rf -- "$EXPECTED"
pnpm test
pnpm smoke:hosts
pnpm smoke:recovery
```

Generator9.0.2 publication, exact root pin/install and actual generation/native ESM import have been observed; the release owner must commit regenerated output and obtain exact-source normal CI. No temporary package/local patch/source-text check substitutes for this reproducibility gate. `pnpm test` includes actual database migration/schema/owner/native/MCP tests and real streaming/concurrency regressions; it must not exclude or skip them. Preserve the exact production-image UID1000/cap0 repeated migration/startup proof and successful source/chart producer gates.

Operator acceptance uses sections4–8: Helm lint/render/negative-input checks, current configuration staging and pinned checkout cases, disposable same-digest initial schema/startup, verified private TLS no-dispatch route, real-provider runner isolation/restart, independent DB/backup/enrollment/control/provider credential rotation and CA overlap/retirement with every owner/client restarted, and protected off-cluster paired restore with held accepted work/no replay. Record source/infra/artifact identity, auth/hostname/unknown-CA and retired-credential rejection, HELD/occupied fingerprints/IDs and actual SDK streaming/concurrency. One successful smoke never substitutes for these gates.

| Current promotion gate | Status to retain until actual evidence exists |
| --- | --- |
| Generated contracts and real ESM modules | Published generator9.0.2 was observed before the rename; regenerate committed output, build and verify current-source CI. |
| Exact-source CI + production-image initial schema/startup + signed image/chart/private release | Record new intended-source success/artifact coordinates; earlier green runs do not qualify. |
| External PostgreSQL/independent secrets/CA/backup + physical placement | Accept actual backend/restore/node placement; a new PVC is not bandwidth isolation. |
| Disposable PostgreSQL install/agent/MCP/rotation/paired recovery | Re-run current-source gates; earlier local host/recovery checks are historical only. |
| Beta GitOps reconcile/private route | **Prepared, not yet deployed/accepted**; production/public/Hindsight unchanged. |
| Public/open-source distribution | Separate owner release: MIT selected, history/redistribution review, branch governance and anonymous source/image/chart install still required. |

See [chart contract](../deploy/helm/coordinator/README.md) and [deployment/Hindsight research](deployment-research.md) for ownership, historical evidence and primary citations.

Primary references: [official signed PGDG/client packages](https://www.postgresql.org/download/linux/debian/), [pg_dump18](https://www.postgresql.org/docs/18/app-pgdump.html), [pg_restore transaction/error flags](https://www.postgresql.org/docs/18/app-pgrestore.html), [libpq verify-full/trust roots](https://www.postgresql.org/docs/18/libpq-ssl.html), [private pgpass](https://www.postgresql.org/docs/18/libpq-pgpass.html), [advisory locks](https://www.postgresql.org/docs/18/explicit-locking.html#ADVISORY-LOCKS), [Kubernetes init ordering](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/), [Recreate](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#recreate-deployment), [Argo waves](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-waves/), [Node24 startup CA](https://nodejs.org/docs/latest-v24.x/api/cli.html#node_extra_ca_certsfile), [Prisma8 native contract/migration CLI](https://docs.prisma.io/docs/orm/v8/).
