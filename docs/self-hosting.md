# Coordinator self-hosting operator guide

## Status and boundaries

This guide describes **private beta 0.2.0-beta.2: prepared, not yet deployed or accepted**, not public production certification. Source, image and chart remain private. The separate beta needs fresh exact-source CI, signed image/chart/private prerelease and disposable install/TLS/rotation/disaster-recovery/observed MCP-streaming gates; historical October12 SQLite/native/KIND and pre-cutover Effect checks do not prove PostgreSQL. Production Coordinator/Hindsight data/resources/endpoints remain untouched. No new check/deployment is claimed here.

The first [beta.1 publication attempt](https://github.com/samuelho-dev/coordinator/actions/runs/38373742895) verified the exact main image, SLSA provenance and SPDX inventory, then uploaded the chart but failed its cosign signing because Helm login did not populate Docker registry authentication. That unsigned beta.1 artifact is not a release and must not be replaced or deployed. Beta.2 uses the existing pinned Docker login action in the publisher before signing; require its new same-source CI and complete signed publication.

MIT is selected in [LICENSE](../LICENSE). Source, image and chart remain private until a separate owner release/visibility decision. [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) records incomplete redistribution review; inspect source history, dependencies, native binary and notices before any public release. Current image builds no longer include the historical complete Git source bundle; old images/bundles need their own audit before redistribution. A license label or metadata spot-check does not certify that audit. Do not publish credentials, protected recovery artifacts or operator configuration.

There is one PostgreSQL broker owner session and optionally one runner with its durable local SQLite journal. No pool, reconnect/retry, HA, safe replay of uncertain native work, automatic promotion, database rollback or automatic apply/merge is promised. The generic chart requires external PostgreSQL18; it never provisions a database server. Start broker-only to establish schema/storage/authentication, then enable a runner only after its TLS, repository, model and isolation prerequisites are ready.

### Private beta channel, not production replacement

The maintainer's isolated operator overlay uses namespace `coordinator-beta`, private Tailnet TLS `coordinator-beta-homelab.taile99f5b.ts.net` and `coordinator-beta-runner-homelab.taile99f5b.ts.net`, no Funnel/public ingress. Database is separate CNPG `cloudnative-pg/coordinator-beta`, app database/role `coordinator_beta`, service `coordinator-beta-rw.cloudnative-pg.svc.cluster.local`. These are downstream operator coordinates, not required independent self-host identities.

Provision new independent credentials through the existing registry/ESO backend item `Dev/coordinator-beta`: DB basicAuth `coordinator-beta-db-credentials` and backup `coordinator-beta-backup` (`ACCESS_KEY_ID`/`ACCESS_SECRET_KEY`). Never copy production/Hindsight secrets or write them in Git/transcripts. Barman target is the operator's actual backend `s3://ai-dev-env/postgres/coordinator-beta/` at `https://s3.us-east-005.backblazeb2.com`; scoped key authentication and capabilities were proven by the bootstrap below, but actual backup/restore remains required before acceptance. It is not the generic chart's application snapshot interface. Production Coordinator and existing Hindsight shared CNPG `ai-dev-env-dev` stay unchanged.

Hindsight is **PostgreSQL with pgvector/selected vector extension**, or embedded **pg0 PostgreSQL** for local use, not SQLite; current documentation also names Oracle AI Database as an enterprise alternative. Its public product/private operator overlay is a useful release-ownership comparison, not authorization to reuse its shared database, credentials or automatic worker retries. See [primary-source Hindsight findings](deployment-research.md#postgresql-sqlite-and-embedded-pg0), [official storage](https://hindsight.vectorize.io/developer/storage) and [external PostgreSQL installation guidance](https://hindsight.vectorize.io/developer/installation#postgresql).

The beta's required `homelab-cp` affinity/control-plane NoSchedule toleration excludes bulk CI/DIND on both ordinary CI worker nodes, **not all CI**: operator `github-actions-runners/values.yaml`371–376 explicitly places the scoped, capacity-one `gha-rs-maintenance` runner on CP with that toleration. The operator must resolve/accept its execution constraints and verify actual admission/templates/node placement before live beta acceptance; this beta does not alter CI placement. **One instance/local-path shares CP root I/O with maintenance CI, etcd and other workloads, no dedicated disk or HA.** Production self-hosting needs dedicated data node/device or external managed PG and measured durability/latency/restore; another PVC/PG alone is not reserved bandwidth or a guaranteed poll-timeout fix. Do not format disks, move existing DBs or reschedule CI in this beta ([CNPG storage](https://cloudnative-pg.io/docs/1.28/storage/), [Kubernetes affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)).

**Activation order:** first review/bootstrap only the beta policy/trust scope, without activating an ApplicationSet, product wrapper, CNPG Cluster/ObjectStore/backup, secret registry or Hindsight/production changes. Then provision the narrow private source Reader and explicit private-package Actions read grants, alongside independent nine-field `Dev/coordinator-beta` 1Password/scoped B2 provisioning through approved main/dev operator tooling (actual provisioning evidence below). Reader remains absent and package permissions remain external prerequisites. Require recorded owner review or owner-configured supported protections; observed infrastructure `dev` is unprotected. Bootstrap dispatch must name exact main SHA and successful normal CI push at that SHA. Only then publish same-source private prerelease, review trusted **beta-only** chart/image/source pins, reconcile ESO/CNPG with verified TLS, stop old owner before migration, reconcile application and perform acceptance. Do not promote stable production wrapper/Hindsight or treat `prepare-private` as promotable.

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
| Storage | Operator owns external PostgreSQL18, least-privilege database role, verified TLS, durable server backup/recovery, and backup/runner-work PVCs with Retain policy and off-cluster protection. Runner work storage must support SQLite WAL/FULL. |
| Native clients | Each host owner installs/reloads the exact Coordinator client with the supported SDK; Kubernetes cannot reload an existing TUI. |

The investigation observed unprotected source and infrastructure `main` branches and a rulesets API 403 under the private plan. Those are historical observations, not proof of current settings or permission to change visibility. Configure review/required checks with an authorized owner when the platform/plan allows; if unreachable, record the missing enforcement and use explicit owner review without claiming branch enforcement. Do not bypass admission or change a plan/repository's visibility as an installation side effect.

The product chart creates ordinary Deployments, ClusterIP Services and PVCs only. It has no common-chart dependency, ingress/gateway, namespace, NetworkPolicy, HPA or provider CRD. The maintainer's homelab overlay (Tailscale/Cilium/MetalLB/1Password/Kyverno/Garage/Mac identities) is an example of a downstream integration, not an upstream dependency. Keep provider-specific manifests and trust identities in your deployment repository. Do not apply the historical homelab chart defaults to an independent installation.

### Supported identity and version matrix

* Server/native runner image: Linux **amd64**, Node **24.21.0**; stock image installs checksum-pinned OMP **18.6.1** at `/usr/local/bin/omp`.
* Supported native SDK/client hosts: Pi **1.0.4**, OMP **18.6.1**. The stock production image installs the real Pi CLI through its production dependency at `/app/node_modules/.bin/pi`; that exact installed CLI passed host smoke. Configure the actual absolute executable in RunnerConfig `launchers` and independently prove pinned repository, model/provider and runner isolation prerequisites before enabling Pi work. Any derivative image still needs its own tested/signed provenance, not a borrowed stock-image digest.
* Broker/runner command transport: stateless MCP **2026-07-28**, official client **2.3.1**. Plugin-owned interactive native control remains stateful **2025-11-25**. Do not combine incompatible command clients/servers.
* Chart/app version `0.2.0-beta.2`; **Effect4.0.3 is stable**, but Prisma **8.0.0-rc.22** and transitive `@prisma/orm-postgres` **8.0.0-rc.17** remain release candidates. This is explicitly a private beta, not a stable Prisma/public production release.
* Full 40-hex source SHA, image `sha256:` digest and chart artifact checksum/OCI digest identify different things. A package version or configured readiness source header does not prove provenance; verify the immutable producer/source linkage.
* Both interfaces use native MCP Streamable HTTP, not deprecated standalone HTTP+SSE. Stateless2026-07-28 commands use **POST-scoped** JSON/SSE (including `subscriptions/listen`), no protocol sessions or GET stream; stateful2025-11-25 plugin control remains separate. Preserve incremental delivery, cancellation/backpressure and individual-message limits, not whole-event-stream buffering/limits. Proxy TLS routes must not silently buffer SSE. Six actual SDK HTTP/SSE/auth/abort tests and a bounded real concurrent POST/SSE progress/cleanup proof passed locally; live-route acceptance remains required. Record actual progress/completions and latency, not configured handler count, saturated-producer backpressure or promised sustained capacity ([exact official specification](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http.md)).

## 2. Acquire one exact release, privately

From an authorized checkout of the intended full source commit, use the existing lockfile; do not upgrade the SDKs or global user profiles:

```sh
pnpm install --frozen-lockfile
pnpm native:build
```

For a prepared release obtain `release-manifest.json`, `coordinator-0.2.0-beta.2.tgz`, license/notices and verification evidence from the release owner. Its source SHA, image repository/digest, CI run, producer and platform must agree. Verify archive SHA-256 against `chartArchiveSha256`; a private archive checksum is not an OCI digest.

The [release workflow](../.github/workflows/release.yaml) must accept the exact source beta version and pass same-source supplementary/chart/image gates before private preparation. `prepare-private` is only artifact preparation, not a published/promotable release. The release owner must subsequently complete actual same-source `publish` at existing private visibility before beta GitOps promotion. Do not borrow old successful CI or infer a release from this document.

`publish` publishes the chart and GitHub release at **existing visibility**, refuses an existing version, signs the chart using the release workflow identity and records the real OCI digest. It never changes visibility or signs the image as a new producer. Public visibility and complete legal/history review require a separate owner action; do not infer public readiness from `publish` success. Do not invent an OCI install command until an actual published, verified chart coordinate is available. For preparation/disposable installation, use the verified local chart archive or exact-source chart directory.

### One declarative schema owner

Prisma8's `prisma/contract.prisma` owns the broker tables, constraints, native Effect queue schema and seeded migration versions. Committed `src/generated/prisma/contract.json`, generated Effect/Kysely types, `prisma.config.ts` and attested `migrations/**` are immutable source/image inputs. Broker startup verifies identity; it never creates/updates schema itself. The image copies those assets and uses the production native Prisma CLI, not a runtime generator. Runner-only SQLite migrations remain separate.

Generated `src/generated/database/types.ts` database/ID/select/insert/update types are authoritative internally. Typed Kysely results are not decoded again. Decode unknown network/config/filesystem/legacy SQLite and serialized TEXT-JSON exactly once at those seams, then carry typed values; schema codecs never replace domain authorization/CAS/fingerprints/recovery fences.

The owner's generic stable Effect4/native ESM fix [PR70](https://github.com/samuelho-dev/prisma-effect-kysely/pull/70) and release preparation [PR71](https://github.com/samuelho-dev/prisma-effect-kysely/pull/71) completed normal publication: [Release38345704625](https://github.com/samuelho-dev/prisma-effect-kysely/actions/runs/38345704625) succeeded and [`prisma-effect-kysely9.0.2`](https://registry.npmjs.org/prisma-effect-kysely/9.0.2) is published. Coordinator's root exact devDependency/lock pin, install, actual generation and native ESM import succeeded. Do not hand-edit generated imports or use old Effect3 infra pins. `pnpm check` runs actual `pnpm db:generate`, imports the generated ESM index and checks types. Normal CI compares all regenerated `src/generated` assets with committed output and fails on drift before its real-PostgreSQL/native gates; generator publication alone is not beta certification.


For schema **authors**, from exact installed tooling and a private URL file if configuration needs it:

```sh
pnpm exec prisma contract emit
pnpm exec prisma-effect-kysely contract \
  --contract src/generated/prisma/contract.json \
  --schema prisma/contract.prisma --output src/generated/database
# Only when authoring a reviewed schema change, not on every install:
pnpm exec prisma migration plan --name "$MIGRATION_NAME"
```

MIGRATION_NAME is the author's reviewed snake_case slug. Commit emitted contract/types and the native attested migration package together; reproduce source generation in CI. Never hand-edit runtime SQL or use Prisma7 migrate deploy, db push, destructive reset/update or schema generation against an installed service. Application is only:

```sh
COORDINATOR_DATABASE_URL_FILE="$PRIVATE/database.url" \
NODE_EXTRA_CA_CERTS="$PRIVATE/ca.crt" \
  node --import tsx scripts/migrate.ts
```

The script acquires the same PostgreSQL owner as the broker and applies native `db migrate --no-interactive` to the exact emitted `contract.storage.storageHash`. Startup and post-migration checks use generated-type Kysely queries executed through the existing Effect SQL client (no second connection pool), verify the actual `prisma_contract.marker` profile/core identity, immutable ledger, native queue v1/v2 seeds and durable recovery gate, and never apply application DDL. It must run with no old broker owner and before new broker serving. Authoring/generation does not apply the package.


## 3. Create private schema-valid configuration

`config.json` in each role Secret is the authoritative existing [BrokerConfig/RunnerConfig](../src/contracts.ts), decoded with excess-property rejection. There is no chart-specific enrollment schema. Owners/principals/machines/runners are your own identifiers (`[a-zA-Z0-9_.:-]`, length 1–160); repositories use lowercase `github.com/owner/repository`; hashes are lowercase SHA-256, pins are full lowercase 40-hex. Enroll only required roles and repository scopes. A no-runner broker needs no task repository or maintainer source checkout.

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
    databaseUrlFile:'/config/database.url',host:'0.0.0.0',port:8787,
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

Run this exact stdin command from the verified source checkout after frozen installation; it uses the existing Effect `Platform` crypto/filesystem service and existing config schemas, not a second configuration model. The exact broker-only example, runner extension below and section4 Helm-values Effect stdin example were executed in disposable private Node24/UID1000 fixtures: all three exited0 with no output. Broker/Runner/Plugin schema checks and three independent credentials/matching hashes passed; all nine generated files0600/UID1000 and private parent0700/UID1000 were verified. Duplicate exclusive write, invalid task SHA and noncanonical PRIVATE each rejected nonzero/zero stdout, leaving existing broker/session/runner bytes unchanged. This is execution/config-boundary evidence only, not verified release/staged deployment. The exact `deploy-init.ts` staging commands in section4 and section7 fourth offline state-count example still require recorded acceptance.

Retention values here are explicit one-day archive/three-day sanitized-backup choices, not recovery guarantees; review them for your policy. This intentionally grants only session registration/polling, not task submission. Add separate `coordinator`, `reviewer`, `archive-writer` or `admin` principals only when their operations are needed, with distinct tokens/scopes. Keep acceptance credentials separate from task-control credentials.

### External PostgreSQL URL and CA

Provision a dedicated empty PostgreSQL18 database and scoped operator-owned login outside the chart; obtain the actual URL through your private secret backend. Do not use a server superuser or expose passwords in shell arguments/Git. Export DATABASE_URL_SOURCE_FILE as the backend's private local export path, then copy without printing bytes:

```sh
umask 077
install -m 600 "$DATABASE_URL_SOURCE_FILE" "$PRIVATE/database.url"
chmod 700 "$PRIVATE"
```

The file contains a PostgreSQL URL with nonempty user/password/database; percent-encode credentials. Remote requires `?sslmode=verify-full`, the only accepted query setting. Plaintext disable is permitted only loopback disposable proofs, never remote Docker/cluster databases. Supply trusted roots as `$PRIVATE/ca.crt`; configure the chart CA Secret to trust PostgreSQL and private HTTPS.

For host migration/recovery/inspection export COORDINATOR_DATABASE_URL_FILE="$PRIVATE/database.url" and NODE_EXTRA_CA_CERTS="$PRIVATE/ca.crt" before startup. Create separate private **host-broker.json/host-runner.json** with actual absolute host paths and the same identities: host databaseUrlFile points at that private file, not Kubernetes-only `/config`; host backupDirectory/state/repository/model/child roots must match the stopped pair. The runtime user owns private0700 parents/0600 files without links. Do not change cluster configs to host paths.


### Runner extension of this configuration

Before enabling the runner, generate **two independent** 32-byte random hex tokens using the same protected file procedure: runner bearer and broker-to-runner control. SHA-256 hashes use trimmed exact raw token bytes. Add a broker principal with `id` equal to RunnerConfig `id`, your owner, `roles: ["runner"]`, exact repository allowlist, runner bearer `tokenHash`, control `controlTokenHash`, and real private HTTPS `runnerUrl`. Broker and runner ports must match chart values. Populate `repositoryIds` with the actual positive GitHub repository numeric IDs; a checkout name is not a repository ID.

RunnerConfig contains exactly these required fields:

| Field | Required operator value |
| --- | --- |
| `id`, `brokerUrl`, `tokenFile` | Enrolled runner ID, verified private HTTPS broker authority, `/config/runner.token`. |
| `host`, `port`, `controlTokenHash` | `0.0.0.0`, normally `8788`, SHA-256 of the separate control token. |
| `stateDirectory`, `modelDirectory`, `childDirectory` | `/work/runner`, `/work/home`, `/work/children` respectively, or distinct nonoverlapping canonical descendants of `/work`. |
| `slots`, `childUidBase` | Integer 1–128 and reserved UID base at least 1000000; allocate nonoverlapping UID ranges among runners. |
| `repositories` | Object mapping every authorized repository to its distinct canonical `/work/repos/...` checkout root, outside all private/child roots. |
| `launchers` | Object mapping `omp` to `/usr/local/bin/omp`, or `pi` to the actual installed executable in your verified image. |

`repositories.json` maps **every and only** configured repository to its exact full commit SHA. Init fetches public GitHub repositories via unauthenticated HTTPS and detaches that pin, without running setup scripts. For private/offline repositories, stop the runner and pre-provision a clean exact checkout on the work PVC using a trusted job and separately scoped clone credentials. Init accepts that checkout without fetching, rejects dirt/mismatch and never resets operator data. Neither image source files nor historical Git bundles are task-repository seeds. Changing pins is an explicit stopped-runner provisioning operation.

For the conventional **one-repository OMP runner**, the following produces actual config/tokens, not a maintainer seed. Export your independently selected `RUNNER_ID`, verified private `RUNNER_URL`, `TASK_REPO`, exact `TASK_SHA`, and actual numeric `TASK_REPO_ID`. This extends the broker-only file **before first install**; an existing install must use the controlled maintenance/rotation procedure below. The million-UID base used here must be reserved on your host.

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
    tokenFile:'/config/runner.token',host:'0.0.0.0',port:8788,controlTokenHash,
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
| Broker | `database.url` | External PostgreSQL URL with nonempty user/password/database, staged at `/config/database.url`; remote URL requires `?sslmode=verify-full`, no other query setting/fragments/control bytes. |
| Broker with runner | `control.<base64url UTF-8 principal.id without padding>` | Raw control token matching that principal's control hash; staged at `/config/tokens/<principal.id>`. |
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
kubectl create namespace "$NAMESPACE"
kubectl -n "$NAMESPACE" create secret generic "$BROKER_SECRET" \
  --from-file=config.json="$PRIVATE/broker.json" \
  --from-file=database.url="$PRIVATE/database.url" \
  --from-file=health.token="$PRIVATE/health.token"
```

For runners add the exact broker control-key entry `--from-file="$CONTROL_KEY=$PRIVATE/control.token"`. For object backup add both backup credential files and configure the HTTPS provider endpoint/region/bucket and private `/config` destinations. Create the runner Secret analogously with its config, matching runner/health token files, `repositories.json` and selected real model seed. File-based `--from-file` arguments expose paths/key names, never raw token bytes. Prefer your existing secret reconciler for steady-state rotation; provider CRDs are not required by the chart. Do not write Secret YAML containing plaintext/base64 secrets to deployment Git or CI artifacts.

For the one-repository OMP runner above, create its Secret with the required files explicitly:

```sh
kubectl -n "$NAMESPACE" create secret generic "$RUNNER_SECRET" \
  --from-file=config.json="$PRIVATE/runner.json" \
  --from-file=runner.token="$PRIVATE/runner.token" \
  --from-file=health.token="$PRIVATE/runner.token" \
  --from-file=repositories.json="$PRIVATE/repositories.json" \
  --from-file=omp.config.yml="$PRIVATE/omp.config.yml"
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

Set `tls.existingCaSecret: <your CA_SECRET name>` in reviewed values before install. Extend default-deny policy to the exact PostgreSQL endpoint/port for **both broker init and runtime**, not only HTTP flows.


Each role exposes `broker.deploymentAnnotations`, `broker.podAnnotations`, `runner.deploymentAnnotations` and `runner.podAnnotations`, all defaulting to `{}` and accepting string maps. Deployment annotations target the Deployment; pod annotations target its pod template. Put operator-owned Secret-reloader or Velero-exclusion annotations at the scope required by your installed controller in the downstream overlay. No provider controller is installed by these maps, and annotations do not replace fenced recovery or the controlled restart procedure.

Default PVCs are `${RELEASE}-backups` (10Gi) and `-work` (20Gi, runner only), RWO. `persistence.backups|work` accepts existingClaim, storageClass, size and accessModes; null storageClass uses cluster default, empty string selects no provisioner. RWOP is supported. The broker has **no data PVC or SQLite runtime flag**. Keep runner WAL on suitable local/block storage. Chart-created PVCs carry Helm keep; existing claims are not Helm-owned. Set PV reclaim policy Retain and protect against GitOps pruning. Independently protect external PostgreSQL storage; keep/Retain/RWO are not backups.

Initializer commands are exactly:

```sh
node --import tsx scripts/deploy-init.ts broker --input /input --config /config
node --import tsx scripts/deploy-init.ts runner --input /input --config /config
```

Root staging has only CHOWN/FOWNER/DAC_OVERRIDE. Secret input is read-only0400; config is memory-backed. Staged files are UID/GID1000 mode0600, private directories0700 and runner child-chain ancestors0711. Broker requires **databaseUrlFile `/config/database.url`** and backupDirectory at/below `/backups`. The fixed second broker init executes `node --import tsx scripts/migrate.ts` on the **same immutable image**, UID1000/capability0, read-only config/CA and writable tmp. `COORDINATOR_DATABASE_URL_FILE=/config/database.url` selects the private secret; failed migration prevents broker startup. There is no skip flag. Both roles use Recreate: the old broker owner stops before migration acquires its lock. Never use an Argo PreSync migration hook against a live owner—it deadlocks rollout ordering.

Runtime root/config filesystems are read-only, tmp writable, service-account automount disabled. Broker runs UID1000/capability0. Runner setpriv becomes UID1000 with exactly CHOWN/DAC_OVERRIDE/KILL/SETUID/SETGID/SETPCAP, **no FOWNER**; children have distinct UIDs, zero capabilities/groups and NoNewPrivs. Do not weaken isolation for admission.

### Local Docker: stage → migrate → broker

Use the same verified image digest, external reachable TLS PostgreSQL18, and private inputs. Prepare `$PRIVATE/docker-input`0700 containing config.json (cluster broker.json), health.token and database.url0600. Export distinct CONFIG_VOLUME, BACKUPS_VOLUME, BROKER_CONTAINER names and CA_DIRECTORY with only trusted ca.crt. These commands mutate the operator's Docker host:

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
  --env COORDINATOR_DATABASE_URL_FILE=/config/database.url \
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
  --env COORDINATOR_RUNNER_TOKEN_DIRECTORY=/config/tokens \
  --env "COORDINATOR_IMAGE_SHA=$SOURCE_SHA" \
  --env NODE_EXTRA_CA_CERTS=/etc/coordinator/ca/ca.crt \
  "$IMAGE"
```

Stop/fence the old container before staging/migrating upgrades. Expose loopback through an operator-owned verified private TLS proxy. Docker has no enforced init dependency: stop on any failure. Use a fresh private config volume for rotation. Runner deployment still requires six-capability setpriv/work preparation/genuine SDK seeds/independent child isolation; follow chart entrypoints rather than privileged containers.


Services `${RELEASE}-broker` and `${RELEASE}-runner` are **internal HTTP ClusterIP**. Operator/provider-owned TLS termination must provide private HTTPS matching `brokerUrl`/`runnerUrl` before enabling the runner; serving certificates, renewal, private keys, DNS and routes remain outside the chart. Configure `tls.existingCaSecret` for private issuers as above; never set insecure verification flags. Keep the HTTPS URL authority as the certificate's real hostname. `connectHosts` may change socket DNS routing, not the URL authority, hostname validation or SNI. Broker-only can start without any host route, but external acceptance requires the real private route.

Create default-deny policies using labels `app.kubernetes.io/name=coordinator`, `app.kubernetes.io/instance=<release>`, `app.kubernetes.io/component=broker|runner`. Explicitly allow DNS, kubelet probes, trusted TLS gateways/clients, runner→broker, broker→runner control, required GitHub fetch/evidence, optional object backup, and selected model/auth endpoints. Prevent native children from obtaining broker/control credentials. TLS gateway/provider policy and child isolation require actual operator tests; a rendered chart does not enforce an unspecified network.

## 5. Disposable install and GitOps promotion

First render/review and run a disposable install using `CHART` as the verified local `coordinator-0.2.0-beta.2.tgz` or exact-source chart directory. Do not invent a public remote release.

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

Broker `/livez` is cheap unauthenticated process liveness; broker `/readyz` is authenticated, performs SELECT1 and reports `x-coordinator-source-sha`. Broker startup/liveness use `/livez` and readiness exec reads `/config/health.header`, not a token argv. Runner has **no `/livez` or `/readyz`**: startup/liveness/readiness all use authenticated curl exec against its existing `/healthz` with the mounted header; journal-ownership loss still fails health. Neither role's `/healthz` becomes unauthenticated/public, and MCP health remains unchanged. A healthy pod does not prove provider auth, repository pins, isolation or no-replay.

Before routing real work, additionally prove runner readiness, clean pinned public/private checkout behavior, a real selected SDK/provider model turn, child credential/UID isolation, broker lock fencing, durable restart/no-replay and protected recovery rehearsal. Do not narrow those to mocks or call successful no-dispatch smoke full runner acceptance.

After disposable acceptance, submit an operator deployment PR pinning the exact verified chart artifact/checksum or published OCI digest, image digest and source SHA together. Include producer/source verification evidence, rendered ordinary APIs, Secret references, PVC retention/ownership, TLS/policy flows, maintenance plan and recorded acceptance. Deployment CI/admission enforce these independently; Argo owns reconciliation after reviewed merge. Source CI cannot certify a foreign operator cluster and this product workflow does not push operator Git, dispatch production workflows or apply the PR. Argo Healthy/Synced is controller state; run the real private-route no-dispatch smoke after sync and record failure rather than silently accepting.

## 6. Client upgrades and maintenance

Install the exact-source client on each native host with `pnpm install --frozen-lockfile && pnpm native:build`, then nonroot `omp plugin link /absolute/coordinator` or `pi install /absolute/coordinator`. PluginConfig defaults to `$XDG_CONFIG_HOME/coordinator/plugin.json` or `~/.config/coordinator/plugin.json`; file/token0600, parent0700. `node --import tsx scripts/install.ts --help` describes the existing `--plugin-only --plugin-config <file>` installer. Plugin-only is a native user action; Linux root `--runner-only` is a separate native service install, not a Kubernetes prerequisite. The operator owns paths; never replace global SDK binaries/profiles or copy a maintainer HOME.

**Reload order for command cutover:** stop new submissions; rebuild/install and fully reload all command clients first, then replace broker/runner servers with the matching source/protocol. Existing TUI processes retain loaded code even after linking/building. Verify actual source/protocol/native identity in fresh sessions; package version alone is inadequate. Old interactive native control remains separate. If versions cannot coexist safely, use maintenance downtime rather than allow mismatched clients to dispatch.

For every change, stop task submission/approved release at all coordinator clients and restrict submission-capable ingress/principals as necessary. There is **no dedicated maintenance-mode/drain endpoint**. A quiet heartbeat or empty no-dispatch smoke is not proof of no work. Inspect known tasks with authorized `task.get`/`task.attach`, reconcile runner state and native children, then stop runner and broker before offline state operations. Retain uncertain/occupied HELD rows; do not clear capacity by editing the database.

Exactly one PostgreSQL broker/migration/recovery owner holds the fixed session advisory lock. Duplicate owners fail closed; owner-session loss fences serving/writes, never reconnects or replays work. This fences the same database, not independent copied databases. Recreate stops the old owner before the migration init; it is not HA. Do not scale replicas or run competing recovery/source brokers. Runner journals retain kernel flock. Reconcile native children independently of pod deletion.

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

There is no all-task maintenance CLI. Use authorized task.get/task.attach for known IDs while serving. After services/children are stopped and fenced, inspect metadata offline with the private PostgreSQL URL file using this no-credential-argv process (local operator working directory, not a cluster exec):

```sh
node --import tsx --input-type=module <<'JS'
import { Console, Effect, Layer, Schema } from 'effect';
import { NodeServices } from '@effect/platform-node';
import { Platform } from './src/platform.ts';
import { BrokerPostgres } from './src/postgres.ts';
import { makeDatabase } from './src/kysely.ts';
import { Task } from './src/contracts.ts';
await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
  const { sql } = yield* BrokerPostgres;
  const db = makeDatabase(sql);
  const counts = new Map();
  for (const row of yield* db.execute(db.client.selectFrom('tasks').select('data'))) {
    const task = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Task))(row.data);
    counts.set(task.state, (counts.get(task.state) ?? 0) + 1);
  }
  yield* Console.log(Object.fromEntries(counts));
})).pipe(
  Effect.provide(BrokerPostgres.layer(process.env.COORDINATOR_DATABASE_URL_FILE)),
  Effect.provide(Layer.merge(Platform.layer, NodeServices.layer))
));
JS
sqlite3 -readonly /work/runner/runner.sqlite \
  "SELECT state,occupied,count(*),sum(pid IS NOT NULL) AS recorded_pids FROM tasks GROUP BY state,occupied;"
sqlite3 -readonly /work/runner/runner.sqlite \
  "SELECT taskId,state,occupied,pid FROM tasks WHERE occupied=1 OR state='HELD' ORDER BY taskId;"
```

Set COORDINATOR_DATABASE_URL_FILE to the canonical private600 file with private700 parent before inspection. Runner paths correspond to this guide's conventional stateDirectory; sqlite3 is an operator-host tool, not bundled. Use the normal owner-accessible filesystem: `-readonly` prevents database writes, but a read-only volume can prevent SQLite from opening WAL support files; never substitute `immutable` against a live journal. Protect inventory output. PIDs do not prove identity or outcome; inspect actual native processes/workspaces/external effects. QUEUED/ASSIGNING/ACCEPTED/RUNNING or occupied rows prevent idle claims; HELD is uncertain, not completed. Preserve fingerprints/incarnations and never force-free/reissue through SQL edits.

## 8. Protected backup and fenced restore

### Application exports versus database disaster recovery

The existing online **application pg_dump snapshots** remove archive/consent/participant-consent rows before local/object export. They still contain ledger/task/evidence/session/queue information and require protected access; “sanitized” does not mean safe to publish. Object storage is optional and operator-owned. `backup-smoke.ts` exercises the sanitized object-store primitive, **not** full runner recovery. Downloading a sanitized snapshot to a new0600 file is not replacing/restoring a production ledger.

CNPG/Barman **physical base backups and WAL/PITR are a separate operator-owned product**, not these archive-sanitized application exports or the paired recovery artifact below. A database restore does not reconcile runner files or fence uncertain accepted work; use the stopped-owner/paired recovery procedure for that boundary. The beta backend's scoped backup/read/restore proof remains required independently of local application recovery.

Protected offline recovery contains **full private** broker/consent/task/session data and paired runner journals/repositories/native files. Its v2 recovery.json binds source/schema identity/counts, runner absolute layout, hashes, UID/GID/modes/internal links. Data files are **broker.pgdump**, optional runner.sqlite and blobs/<decimal index>. PostgreSQL native schema marker/history and recovery_gate data are excluded: restore first applies the **same committed migration package** to a fresh empty target. Artifacts are private600/700, not automatically encrypted; encrypt/off-cluster protect and independently escrow excluded credentials.

Operator configs/enrollment/control tokens/model seed and known copied child model-auth/config/native-control token/plugin/endpoint files are excluded. External native host plugin journals/history are **operator-owned and excluded**: no resumed/recovered peer session or native-control reattachment is promised. Other application files can contain secrets: treat **every byte** of the protected artifact as confidential. Reconstruct excluded credentials/model seed independently from the private secret backend; keep config files outside archived repository/child roots.

### Backup command

Suspend reconciliation; close submissions; stop broker, runner and native descendants; prove old ownership and effects are fenced. The command never stops services for you. Use the **same exact release source** and installed dependencies. Recovery file/config/artifact paths are canonical absolute paths with private owner0700 parents; configuration files0600 owned by the invoking user. The only narrow exception is the runner state directory: its leaf must be0700 and its operator-owned immediate parent may be0700 or0711, matching `/work` traversal;0751 is rejected. Artifact/config/database parents remain strict private0700. Ancestors cannot be unsafe/symlinked. The artifact directory must not already exist. Runner files may need root access to preserve distinct child UID ownership; run in a controlled offline admin environment with the same volume paths, not a generic network-facing service.

Recovery filesystem operations and bounded file streams use the existing Effect Platform and scoped handles; durable writes still fsync files and their parent directories. The pinned Effect API has no `lstat`/`lchown` or incremental digest, so recovery invokes fixed Linux coreutils through Effect ChildProcess: `/usr/bin/stat` for non-following metadata, `/usr/bin/chown --no-dereference` for link ownership, and `/usr/bin/sha256sum` with streamed stdin. These binaries are required alongside `flock`/`setpriv`; no new JavaScript dependency or runner FOWNER capability is needed.

```sh
node --import tsx scripts/recover.ts backup \
  --broker-config "$PRIVATE/host-broker.json" \
  --runner-config "$PRIVATE/host-runner.json" \
  --artifact "$RECOVERY_ARTIFACT" --source-sha "$SOURCE_SHA" \
  --offline-maintenance
```

Omit runner config only for genuine broker-only topology; nonterminal assigned work needs its matching runner. Multi-runner paired recovery is unsupported by this single-runner command. Helpers acquire the PostgreSQL owner and runner.lock, reject live/unknown children and unsafe paths/links/files. They never stop services, delete source state or replay work. No credentials appear in native client argv; temporary pgpass files are private scoped600 files.

### Restore command and explicit accepted-work hold

1. Keep source services/children offline and independently fence old endpoints/databases/volumes, including copies on other machines. An advisory lock cannot fence another copied database.
2. Provision a **fresh empty external PostgreSQL database** and fresh runner volumes at identical absolute paths/runner identity/UID base; retain old state untouched. Reconstruct private target BrokerConfig/URL/model credentials. Apply the exact committed migration package using the command below **before restore**, not by starting the broker. Runner SQLite target must remain absent without stale sidecars.
3. Recover the protected artifact securely. Recovery/import durably sets recovery_gate false before mutation and only true after the paired operation completes. Any partial failure stays fenced: quarantine it and retry into a separately fresh target, never bypass the gate.
4. Inspect protected manifest `broker.pendingAccepted` and optional `runner.pendingAccepted` offline. Broker ACCEPTED/RUNNING tasks could redeliver peer release and pending occupied runner ACCEPTED rows would ordinarily auto-launch. Restore requires explicit **`--hold-accepted-work`** acknowledgment when either count is nonzero; it parks copied broker ACCEPTED/RUNNING tasks and pending runner ACCEPTED rows HELD before startup, preserving acceptance/release IDs, fingerprints, work/incarnations and offer decisions. This is not consent to execute them, and external host journals remain excluded.

Apply the exact source package to the fresh target using its host private URL:

```sh
COORDINATOR_DATABASE_URL_FILE="$PRIVATE/database.url" \
NODE_EXTRA_CA_CERTS="$PRIVATE/ca.crt" \
  node --import tsx scripts/migrate.ts
```


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

Run only on a disposable Linux amd64 host/container with Node24, PostgreSQL18 tooling/fresh fixture databases, dependencies, root/six supervisor capabilities, flock/setpriv and recovery coreutils, no production mounts/credentials. The rehearsal uses real broker/runner services and local fixture launcher, not a model provider; parent must execute the PostgreSQL conversion gate. Historical SQLite rehearsal does not certify this beta.

Image reverts never downgrade/mend data or undo external effects. Incompatible schemas require reviewed migration/recovery rehearsal; same-source pairing is not arbitrary N−1 migration or HA. Accept downtime rather than replay.

### Offline legacy SQLite import

One-time offline conversion, not a dual broker backend/flag. Stop/fence the legacy broker, paired runner and native children; retain original SQLite files/work unchanged. Prepare a fresh exactly migrated PostgreSQL target and private host configs. The CLI locks/checkpoints the canonical private source, requires no stale sidecars/unproven writer, verifies supported legacy schema and preserves IDs/queue/acceptance/fingerprints through paired protected recovery:

```sh
COORDINATOR_DATABASE_URL_FILE="$PRIVATE/database.url" \
NODE_EXTRA_CA_CERTS="$PRIVATE/ca.crt" \
  node --import tsx scripts/migrate.ts
node --import tsx scripts/import-sqlite.ts \
  --source "$LEGACY_SQLITE_FILE" \
  --broker-config "$PRIVATE/host-broker.json" \
  --runner-config "$PRIVATE/host-runner.json" \
  --artifact "$IMPORT_ARTIFACT" --source-sha "$SOURCE_SHA" \
  --offline-maintenance --hold-accepted-work
```

LEGACY_SQLITE_FILE is the canonical private original; IMPORT_ARTIFACT is a new private700-parent path. Omit runner config only for actual broker-only data. Explicit hold acknowledgment parks copied accepted work HELD; never clears occupied uncertainty or grants execution. Failed partial targets stay recovery-fenced; never bypass the gate/delete source. Inventory preserved outcomes and prove private no-dispatch acceptance before reopening.

For the parent/operator **disposable** real-PostgreSQL import gate, with a private dedicated admin URL file (never production credentials):

```sh
COORDINATOR_TEST_POSTGRES_URL_FILE="$PRIVATE/disposable-admin.url" \
  node --import tsx scripts/import-smoke.ts
```

Use the same isolated Linux/tooling/six-capability conditions as recovery smoke. The proof covers committed WAL fixtures, supported native queue versions and owner/empty-target/accepted-work guards; record actual results separately, not an unexecuted-command claim.



## 9. Evidence required before release/promotion

Record exact source SHA, successful trusted-source CI run, image digest/signature/provenance/SBOM, chart version and archive checksum (OCI digest only after actual publication), supported client/SDK versions and freshly reloaded client identity. Separately record disposable clean install, private TLS/auth rejection/SELECT1/no-dispatch evidence, runner independent pinned checkouts/real provider model turn/isolation, duplicate broker fencing, durable restart/no-replay, protected recovery rehearsal and actual off-cluster retrieval/secret reconstruction. Do not print raw config/tokens/DB contents or publicize full recovery metadata.

Current beta remains **prepared, not live**. See the [current gate ledger](deployment-research.md#current-gate-ledger-requirements-not-unobserved-success): generator9.0.2 publication/root generation/native ESM and latest local Effect/generated/native build checks passed; corrected filtered32 broker/runner tests passed; actual stock Pi1.0.4/OMP18.6.1 host controls passed54 fixture model calls/zero external calls; protected offline paired recovery and committed-WAL SQLite→PostgreSQL v1/v2 import passed, preserving BigInt/microseconds/0–1/UUID/no replay. The corrected complete unfiltered native suite **passed89/89 with zero failures/cancellations/skips/todos in575.17s**, superseding the earlier62pass/27fail run. Current source/fresh-dist disposable snapshot is not a release source SHA. PostgreSQL18.6/tmpfs/Node24/UID1000/six-capability fixtures do not establish physical-disk/PVC, off-cluster retrieval, production-image, live TLS/credential/CA-rotation or reconciliation acceptance. Bounded real SDK POST/SSE progress/cleanup passed, not sustained capacity/saturated-backpressure/exact private-scope counts. Reader remains absent and must be environment-only in `coordinator-beta-promotion`; package Actions read grants remain unverified external prerequisites.

Outstanding prerequisites are concrete: authorized private artifacts/source access; completed source/history/redistribution review for any public release; owner-controlled source/deployment branch protection where platform permits; cluster admission trust configuration; operator Secret/TLS/DNS/model/repository/storage inputs; and recorded main/operator gates. None is supplied by a fake sample credential, `.invalid` model, historical homelab Healthy status or unexecuted command.

Release-owner validation commands below are requirements, **not executed by this documentation update**. Run them only against disposable fixtures with the existing isolated Linux UID1000/six-capability/private-HOME environment and private PostgreSQL18 admin URL; stock Pi/OMP smoke additionally needs the pinned CLI paths/version environment described above. The normal CI workflow supplies those prerequisites and retains the entire suite, original600s native deadline and production-image migration gate:

```sh
pnpm install --frozen-lockfile
EXPECTED="$(mktemp -d)"
cp -a src/generated/. "$EXPECTED/"
pnpm check
diff --recursive --unified "$EXPECTED" src/generated
rm -rf -- "$EXPECTED"
COORDINATOR_TEST_POSTGRES_URL_FILE="$PRIVATE/disposable-admin.url" pnpm test
COORDINATOR_TEST_POSTGRES_URL_FILE="$PRIVATE/disposable-admin.url" pnpm smoke:hosts
COORDINATOR_TEST_POSTGRES_URL_FILE="$PRIVATE/disposable-admin.url" pnpm smoke:recovery
COORDINATOR_TEST_POSTGRES_URL_FILE="$PRIVATE/disposable-admin.url" pnpm smoke:import
```

Generator9.0.2 publication, exact root pin/install and actual generation/native ESM import have been observed; the release owner must commit regenerated output and obtain exact-source normal CI. No temporary package/local patch/source-text check substitutes for this reproducibility gate. `pnpm test` includes actual database migration/schema/owner/native/MCP tests and real streaming/concurrency regressions; it must not exclude or skip them. Preserve the exact production-image UID1000/cap0 repeated migration/startup proof and successful source/chart producer gates.

Operator acceptance then uses the explicit commands in sections4–8: Helm strict lint/render/negative-input checks, real config staging and private/pre-provisioned checkout cases, disposable same-digest migration/startup, private verified TLS `deployment-smoke`, runner real-provider/child isolation/restart, independent DB/backup/enrollment/control/provider credential rotation and CA overlap/retirement with broker/runner/client restarts, paired protected off-cluster restore and legacy import with held accepted work/no replay. Record source/infra/artifact identity, auth/hostname/unknown-CA rejection, old-credential rejection, retained HELD/occupied fingerprints/IDs, actual observed SDK streaming/concurrency and zero unintended dispatch. A successful one-shot smoke is not sustained-throughput proof; record completed calls, concurrent in-flight progress and latency/elapsed time under slow-stream pressure with redacted output.

| Current promotion gate | Status to retain until actual evidence exists |
| --- | --- |
| Generator normal release/pin + regenerated contracts | Published9.0.2, exact root pin/install/generation/native ESM and latest local checks passed; committed-output/same-source normal CI remains required. |
| Exact-source normal CI + production image migration + signed image/chart/private prerelease | Prepared gates; intended-source success/artifact coordinates must be recorded. |
| Isolated CNPG/independent secret/CA/backup + CP placement | Prepared operator intent; actual backend access/restore/node placement must be accepted. |
| Disposable PostgreSQL install/native/MCP/rotation/paired recovery/import | Complete unfiltered89/89 with zero skips, filtered32, stock-host54/zero-external, offline paired recovery and v1/v2 WAL import passed locally. Physical-storage/off-cluster/live rotation/install gates remain required. |
| Beta GitOps reconcile/private route | **Prepared, not yet deployed/accepted**; production/public/Hindsight unchanged. |
| Public/open-source distribution | Separate owner release: MIT selected, history/redistribution review, branch governance and anonymous source/image/chart install still required. |

See [chart contract](../deploy/helm/coordinator/README.md) and [deployment/Hindsight research](deployment-research.md) for ownership, historical evidence and primary citations.

Primary references: [official signed PGDG/client packages](https://www.postgresql.org/download/linux/debian/), [pg_dump18](https://www.postgresql.org/docs/18/app-pgdump.html), [pg_restore transaction/error flags](https://www.postgresql.org/docs/18/app-pgrestore.html), [libpq verify-full/trust roots](https://www.postgresql.org/docs/18/libpq-ssl.html), [private pgpass](https://www.postgresql.org/docs/18/libpq-pgpass.html), [advisory locks](https://www.postgresql.org/docs/18/explicit-locking.html#ADVISORY-LOCKS), [Kubernetes init ordering](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/), [Recreate](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#recreate-deployment), [Argo waves](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-waves/), [Node24 startup CA](https://nodejs.org/docs/latest-v24.x/api/cli.html#node_extra_ca_certsfile), [Prisma8 native contract/migration CLI](https://docs.prisma.io/docs/orm/v8/).
