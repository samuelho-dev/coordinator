FROM node:24.21.0-bookworm-slim AS native-build
RUN npm install --global pnpm@11.9.0
WORKDIR /build
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json ./
COPY packages/native-control ./packages/native-control
COPY src ./src
RUN pnpm install --frozen-lockfile && pnpm run native:build && chmod -R a+rX /build

FROM node:24.21.0-bookworm-slim
LABEL org.opencontainers.image.licenses="MIT"
# Bootstrap on 443-only networks with Node's trusted roots; apt still verifies TLS and repository signatures.
RUN node -e "const fs = require('node:fs'); \
    const ca = '/etc/ssl/certs/ca-certificates.crt'; \
    if (!fs.existsSync(ca)) { fs.mkdirSync('/etc/ssl/certs', { recursive: true }); fs.writeFileSync(ca, require('node:tls').rootCertificates.join('\n') + '\n'); } \
    const sources = ['/etc/apt/sources.list', ...fs.readdirSync('/etc/apt/sources.list.d').filter(name => /\.(list|sources)$/.test(name)).map(name => '/etc/apt/sources.list.d/' + name)]; \
    for (const path of sources) { if (fs.existsSync(path)) fs.writeFileSync(path, fs.readFileSync(path, 'utf8').replaceAll('http://deb.debian.org/', 'https://deb.debian.org/')); }" \
    && apt-get update -o APT::Update::Error-Mode=any \
    && apt-get install -y --no-install-recommends ca-certificates curl git util-linux \
    && rm -rf /var/lib/apt/lists/*
# PGDG's signed repository supplies only the PostgreSQL 18 client, never a server.
RUN install -d /usr/share/postgresql-common/pgdg \
    && curl --fail --silent --show-error --location https://www.postgresql.org/media/keys/ACCC4CF8.asc --output /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && printf '%s\n' 'Types: deb' 'URIs: https://apt.postgresql.org/pub/repos/apt' 'Suites: bookworm-pgdg' 'Architectures: amd64' 'Components: main' 'Signed-By: /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc' > /etc/apt/sources.list.d/pgdg.sources \
    && apt-get update -o APT::Update::Error-Mode=any \
    && apt-get install -y --no-install-recommends postgresql-client-18 \
    && /usr/bin/pg_dump --version && /usr/bin/pg_restore --version \
    && rm -rf /var/lib/apt/lists/*
RUN npm install --global pnpm@11.9.0
RUN curl --fail --silent --show-error --location https://github.com/can1357/oh-my-pi/releases/download/v18.6.1/omp-linux-x64 --output /usr/local/bin/omp \
    && echo 'c92a6846d02984e84f07c6362d18add3783f528f494ffcf1e0f26e594f327463  /usr/local/bin/omp' | sha256sum --check \
    && chmod 0755 /usr/local/bin/omp && omp --version
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/native-control/package.json ./packages/native-control/package.json
COPY --from=native-build /build/packages/native-control/dist ./packages/native-control/dist
COPY --from=native-build /build/dist/plugins ./dist/plugins
RUN pnpm install --prod --frozen-lockfile
COPY src ./src
COPY scripts ./scripts
COPY prisma.config.ts ./
COPY prisma ./prisma
COPY migrations ./migrations
COPY deploy/helm/coordinator ./deploy/helm/coordinator
COPY LICENSE THIRD_PARTY_NOTICES.md ./
RUN mkdir -p /usr/local/share/coordinator/third-party \
    && cp node_modules/@oh-my-pi/pi-coding-agent/LICENSE /usr/local/share/coordinator/third-party/OMP-LICENSE \
    && cp node_modules/@oh-my-pi/pi-coding-agent/THIRD-PARTY-NOTICES.txt /usr/local/share/coordinator/third-party/OMP-THIRD-PARTY-NOTICES.txt \
    && cp LICENSE THIRD_PARTY_NOTICES.md /usr/local/share/coordinator/
RUN chmod -R a+rX /app
RUN node --import tsx --input-type=module -e "import{Context,Effect,Layer}from'effect';import*as S from'@effect/sql-sqlite-node/SqliteClient';await Effect.runPromise(Effect.scoped(Effect.gen(function*(){const c=yield*Layer.build(S.layer({filename:':memory:'}));yield*Context.get(c,S.SqliteClient).unsafe('SELECT 1')})))"
USER node
ENV NODE_ENV=production
CMD ["node", "--import", "tsx", "src/main.ts", "broker"]
