# Coordinator chart 0.2.0-beta.3

**Private beta, not public production certification.** External PostgreSQL18 is required; no database server/provider CRD/common-chart/ingress/namespace/HPA/public endpoint is installed. No publication, visibility change or deployment is implied. Broker-only is supported with runner disabled and no runner principals. Effect4.0.3 is stable; Prisma8.0.0-rc.22 and transitive orm-postgres8.0.0-rc.17 are RC dependencies.

## Release and storage inputs

Supply `image.repository`, `image.digest` (`sha256:` plus 64 lowercase hex digits), and `sourceSha` (the matching full 40-character Git commit). Obtain both coordinates from the same verified release; syntax validation cannot prove that a registry digest contains a particular commit. `COORDINATOR_IMAGE_SHA` is passed to the runtime and reported by readiness. Images currently target Linux amd64; the chart selects that architecture.

Required: `broker.existingSecret`. With `runner.enabled: true`, also require `runner.existingSecret`. Secrets must already exist in the release namespace. Helm validates input names and release coordinates; Kubernetes refuses missing Secrets, and the initializer refuses missing keys, malformed schemas, or credential mismatches. Offline rendering does not inspect Secret contents.

Set `broker.port`/`runner.port` to the corresponding config port (defaults 8787/8788). Each role accepts `resources`, `connectHosts` (TLS hostname → connection DNS override), and `extraEnv` (Kubernetes EnvVar list, including secretKeyRef). Reserved runtime variables cannot be overridden. For broker GitHub reads, set the environment name selected by `BrokerConfig.githubTokenEnv` through `broker.extraEnv` using a separate existing Secret; no GitHub credential is implicitly installed or shared with the runner. Additional host SDK authentication, if used, similarly belongs in explicit runner `extraEnv` inputs.

Each role also accepts `deploymentAnnotations` and `podAnnotations` as string maps (both default `{}`). They apply to Deployment metadata and pod-template metadata respectively; use them for operator-owned Secret-reload controllers, backup exclusions, or lifecycle/privacy ownership without copying workloads. The chart retains its automatic pod `checksum/values` annotation; that key cannot be supplied through `podAnnotations`. Secret contents are not included in that checksum, and annotations alone do not install a controller.

`nodeSelector` and the native `tolerations` array apply to both roles; defaults retain amd64 selection and tolerate no taints. Kubernetes owns toleration-field validation. Selecting a tainted data/control-plane node without its matching toleration leaves Pods Pending; use the operator's actual taint, not a global admission exception:

```yaml
nodeSelector:
  kubernetes.io/hostname: homelab-cp
tolerations:
  - key: node-role.kubernetes.io/control-plane
    operator: Exists
    effect: NoSchedule
```

[Native taints/tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/) permit scheduling, not physical storage isolation, security elevation or HA.

`persistence.backups` and `.work` expose existingClaim, storageClass, size and accessModes; defaults create10Gi backups and20Gi work (runner only), ReadWriteOnce/RWOP. Null storageClass uses cluster default; empty string selects no provisioner. Claims have Helm keep; existing claims are not Helm-owned. Set PV Retain and protect external PostgreSQL plus runner WAL/backups off-cluster. **No broker data PVC/mount/value exists.** There is one broker and at most one runner, Recreate/no HPA/60s termination. Advisory ownership fences the same database, not restored copies.

## Existing Secret keys

Both role Secrets require:

| Key | Contents |
| --- | --- |
| `config.json` | Complete existing `BrokerConfig` or `RunnerConfig` JSON, decoded with excess-property rejection. No alternate enrollment schema. |
| `health.token` | 64 lowercase hexadecimal characters, with optional trailing whitespace. Broker: matches an enrolled principal's tokenHash. Runner: equals runner.token. |

Broker-only additional keys:
* **Required `database.url`**: external PostgreSQL URL with nonempty user/password/database. Remote requires `?sslmode=verify-full`; only sslmode is accepted as a query setting. No fragments/control bytes. Staged `/config/database.url` is UID1000/0600 within private0700 config; never put the DSN in JSON/values/argv/logs.

* For each principal with role `runner`, `control.<base64url(principal.id)>`: raw 64-hex control token matching its `controlTokenHash`. Base64url encodes UTF-8 without padding (for example `node -p 'Buffer.from(process.argv[1]).toString("base64url")' "$RUNNER_ID"`). Config principals require explicit private HTTPS `runnerUrl`. The staged path is `/config/tokens/<principal.id>`.
* When `objectBackup` is configured, `backup.access-key` and `backup.secret-key`: exact provider values. Their config destinations must be descendants of `/config`. Object backup is optional; neither Garage nor any other provider is required.

Runner-only additional keys:

| Key | Contents |
| --- | --- |
| `runner.token` | Raw broker-enrolled runner token (not the control token). |
| `repositories.json` | JSON object mapping **every and only** configured repository name to an explicit full commit SHA. |
| `omp.config.yml` | Required with an omp launcher: JSON accepted by existing ModelProfile, despite the filename (modelRoles.default selects the real operator model). |
| `pi.settings.json` | Required with a pi launcher: actual SDK settings JSON. |
| `omp.auth.json`, `pi.auth.json` | Provider credentials in the selected SDK's genuine auth format when that provider requires credentials. Optional for genuinely credential-free local providers. |
| `omp.models.json`, `pi.models.json` | Optional genuine SDK custom provider/model definitions, including local-provider endpoints. |

Seed only model settings/auth/definitions, never sessions/history/prompts/plugin enrollment or unrelated HOME. Init validates JSON, not provider semantics; require a real admission/model-turn gate. Stock image contains checksum-pinned `/usr/local/bin/omp` and real production Pi at `/app/node_modules/.bin/pi`. Derivative images need their own source/signature/SDK evidence. Nothing is installed from a shell URL during pod startup.

Generate tokens with a cryptographically secure generator, hash their **trimmed exact raw bytes** with SHA-256 into the existing config's tokenHash/controlTokenHash fields, and scope principals/repositoryIds/roles using normal enrollment policy. Keep source files private (umask 077); never put Secret contents in chart values, shell arguments, release metadata, Git, or CI logs. A file-based Secret creation pattern is:

```sh
kubectl -n "$NAMESPACE" create secret generic "$BROKER_SECRET" \
  --from-file=config.json="$BROKER_CONFIG_FILE" \
  --from-file=database.url="$DATABASE_URL_FILE" \
  --from-file=health.token="$BROKER_HEALTH_TOKEN_FILE"
```

For a broker with runners or object backup, add the exact additional `--from-file=KEY=PRIVATE_FILE` entries above. Build the runner Secret analogously, including repositories.json, runner.token, health.token and the real selected model seed. Those commands mutate the target namespace: execute only as an explicit operator deployment step, not during private preparation.

## Config paths and repository preparation

Broker config must use host0.0.0.0, **databaseUrlFile `/config/database.url` exactly**, and backupDirectory at/below `/backups`. Retention/principals/repository IDs/optional HTTPS backup remain BrokerConfig. No broker SQLite compatibility field/flag exists. Remote PostgreSQL requires verified TLS and an operator-owned database role with rights to apply the committed package and serve the broker, never a shared superuser role.

Runner config must use host `0.0.0.0`, an HTTPS brokerUrl whose certificate is valid for that hostname, and tokenFile under `/config` (normally `/config/runner.token`). Set stateDirectory, modelDirectory, childDirectory, and all repository paths to **distinct, nonoverlapping canonical descendants of `/work`**. Conventional values are `/work/runner`, `/work/home`, `/work/children`, and `/work/repos/<operator-name>`. Slots, childUidBase (at least 1000000), controlTokenHash, identity, allowlisted repositories, and installed executable launchers come directly from RunnerConfig. The child root is separate from every private directory.

For each github.com/OWNER/REPOSITORY path, init creates an empty checkout, fetches the explicit SHA over verified GitHub HTTPS and checks out detached, never running setup scripts. Task repositories are independent operator inputs, not image source/maintainer seeds. NetworkPolicy must permit init DNS/HTTPS.

For private/offline repositories pre-provision a trusted clean checkout on the work PVC with scoped credentials, recording its exact SHA in repositories.json. Init accepts the clean exact pin without fetch and rejects dirt/mismatch without resetting operator data. Change pins only with the runner stopped. Current images omit the historical complete Git source bundle; immutable signatures/provenance and authorized exact-source checkout provide release trust, not a bundled maintainer checkout.

Prepared PVC contents, `.git` metadata, bootstrap jobs, and image/global Git configuration are administrator-trusted inputs to the root initializer. A clean worktree and matching HEAD are commit checks, not Git-metadata sanitization: Git configuration, hooks, fsmonitor, filters, and templates can execute operator-configured programs. Never import an untrusted `.git` directory, Git configuration, or checkout PVC; use a trusted bootstrap from repository objects instead. The chart does not treat user-supplied Git metadata as a sandbox boundary.

The root init command is exactly:

```sh
node --import tsx scripts/deploy-init.ts broker --input /input --config /config
node --import tsx scripts/deploy-init.ts runner --input /input --config /config
```

Broker then has a **fixed second init**, same repository@digest:

```sh
COORDINATOR_DATABASE_URL_FILE=/config/database.url node --import tsx scripts/migrate.ts
```

It runs UID1000/cap0 with config/CA read-only and writable tmp. Actual native Prisma migration failure prevents serving; no optional skip flag. Recreate stops the old broker before migration acquires the same advisory owner. **No PreSync migration hook:** Argo must reconcile the Recreate Deployment and await its ordinary init gate, not wait on a migration while the old owner is live.

`/input` is the read-only role Secret (0400). `/config` is an in-memory emptyDir; configs, tokens, health.header and backup credential files are staged as UID/GID1000 mode0600, with private directories0700. Runner seed files are likewise0600 on the work PVC; known seed files removed from the Secret are removed from the staged seed, preventing stale auth reuse. `/work` and child ancestors permit traversal0711, without exposing private0700 siblings. Existing checkouts are recursively owned by1000 without following symlink targets. Runtime mounts config read-only, durable role PVCs writable, and a private writable `/tmp`; the root filesystem is read-only and service-account token mounting is disabled.

Staging walks mount ancestors, rejects symlink/non-directory ancestors, and assigns UID/GID1000 throughout. Private config/backups/state/repository/model ancestors remain0700. Only work and the child-root chain are0711; no general0755 workaround. Existing files are not reset by ancestor repair.

The broker runs directly as1000 with all capabilities dropped. Runner startup uses root setpriv to become1000 with exactly CHOWN, DAC_OVERRIDE, KILL, SETUID, SETGID, SETPCAP; the supervisor retains its existing distinct child-UID/capability/no-new-privileges isolation. Init has only CHOWN, FOWNER, DAC_OVERRIDE. The cluster policy must permit these intentional capabilities on the runner; do not weaken child isolation to satisfy a restricted policy.

## TLS and network-policy integration ownership

Both Services are internal ClusterIP HTTP services; no host is required for broker-only startup. They are not public HTTPS endpoints. The operator owns TLS termination/proxying, valid private certificates/trust, DNS, any gateway/provider resources, and NetworkPolicies. Before enabling runners, provide real private HTTPS routes to broker and runner control endpoints, matching brokerUrl and principal.runnerUrl, with TLS verification intact. Never disable verification.

`tls.existingCaSecret` names an existing Secret with ca.crt PEM trust roots, no private key. The read-only directory at `/etc/coordinator/ca` supplies NODE_EXTRA_CA_CERTS to migration, broker and enabled runner. It must trust both PostgreSQL and private HTTPS authorities; native dump/restore maps it to PGSSLROOTCERT. Remote database URLs require verify-full. Empty default retains normal public CA trust. The variable is reserved from extraEnv; no inline certificate/key or issued certificate is provided.

The added CA extends Node runtime trust; certificate chain, hostname and SNI verification remain enabled, including with `connectHosts` DNS overrides. It does not configure the root initializer's Git fetch, curl, or arbitrary SDK/child TLS stacks: provision trust separately for those when required. Node reads the CA file at process startup, so Kubernetes Secret projection updates alone do not reload trust; the operator must arrange a controlled pod restart/rollout after rotation (for example through the role annotations and an independently installed Secret-reload controller). Role config Secrets are likewise staged only by init. Private CA, config Secret and image rotation remain operator-owned lifecycle decisions; do not interrupt accepted work or replay it as a reload procedure.

Use app.kubernetes.io name/instance/component labels for default deny and explicit DNS/probes/private gateways, **broker init/runtime→PostgreSQL**, broker↔runner, optional backup/GitHub, init repository fetch and selected SDK provider/auth flows. Database trust/availability, gateway/network policy and connectHosts routing belong to operators. Do not expose native control or broker credentials to children; chart presumes no provider/maintainer hosts.

Broker startup/liveness use cheap unauthenticated `/livez`; broker readiness uses curl with `@/config/health.header` against authenticated `/readyz`, never a token in argv. Broker readiness includes the authenticated SELECT1 gate and returns non-2xx on failure; successful readiness includes `x-coordinator-source-sha`. Runner startup, liveness, and readiness instead use authenticated curl exec probes against its existing `/healthz`, with the same mounted header file; the runner does not implement `/livez` or `/readyz`. Runner journal-ownership loss still fails health. Neither role's `/healthz` is exposed publicly or made unauthenticated; MCP health remains unchanged.

## Parent/operator gates (not executed during preparation)

The runnable `check-ancestor-access.mjs` regression consumes the **actual installed runner config and mounts**, not a config fixture or source-text assertion. Run it against the disposable operator installation after init with the exact runtime UID1000/six-capability command; it proves readable staged seed files, traversable/writable configured directories, child-root traversal, and child denial of private directories/tokens:

```sh
kubectl exec -i -n "$NAMESPACE" "$RUNNER_POD" -- /usr/bin/setpriv \
  --reuid=1000 --regid=1000 --clear-groups \
  --inh-caps=+chown,+dac_override,+kill,+setuid,+setgid,+setpcap \
  --ambient-caps=+chown,+dac_override,+kill,+setuid,+setgid,+setpcap \
  --no-new-privs node --import tsx --input-type=module \
  < deploy/helm/coordinator/check-ancestor-access.mjs
```

This uses the reviewed chart-source script through stdin while Node resolves the installed SDK/contracts from `/app`; it does not depend on the script's installed image path. The program exits nonzero if run as root or without exactly the six effective supervisor capabilities. It does not enroll or dispatch work.

```sh
helm lint deploy/helm/coordinator -f "$OPERATOR_VALUES"
helm template "$RELEASE" deploy/helm/coordinator -f "$OPERATOR_VALUES" > "$RENDERED_MANIFEST"
helm template "$RELEASE" deploy/helm/coordinator -f "$OPERATOR_VALUES" --set runner.enabled=true --set runner.existingSecret="$RUNNER_SECRET"
```

Negative rendering checks must reject absent/malformed digest/SHA, missing broker Secret name, and enabled runner without its Secret name. Inspect rendered broker-only and broker+runner workloads with only ordinary Kubernetes APIs. Main should run the repository type/lint/native-isolation gates and actual init tests with independently generated schema-valid broker/runner inputs. Operator acceptance additionally requires valid credential readiness, raw-ledger ownership fencing, durable restart/no-replay evidence, clean public/private repository preparation, real selected SDK/provider model turn, private verified TLS, and policy-enforced child isolation. No build, rendering, lint, test, cluster action or publication was performed by this chart task.

## Concrete installation and recovery process

Follow the [operator guide](../../../docs/self-hosting.md) for independently generated identities, private URL-file/CA provisioning, local Docker staging→migration→broker, Kubernetes Secret creation→Recreate/init→private acceptance, and operator-owned GitOps ordering. It includes exact-source generation, offline backup/restore and legacy SQLite import commands. Prisma owns the broker schema; Effect's queue schema/version seeds are part of the committed initial package, not a second runtime schema owner.

Primary platform contracts: [Kubernetes init ordering](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/), [Recreate Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#recreate-deployment), [official signed PGDG/client packages](https://www.postgresql.org/download/linux/debian/), [PostgreSQL TLS verification](https://www.postgresql.org/docs/18/libpq-ssl.html), [Node startup CA trust](https://nodejs.org/docs/latest-v24.x/api/cli.html#node_extra_ca_certsfile).
