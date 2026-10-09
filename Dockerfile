FROM node:24.21.0-bookworm-slim AS native-build
RUN npm install --global pnpm@11.9.0
WORKDIR /build
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json ./
COPY packages/native-control ./packages/native-control
COPY src ./src
RUN pnpm install --frozen-lockfile && pnpm run native:build && chmod -R a+rX /build

FROM node:24.21.0-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git util-linux && rm -rf /var/lib/apt/lists/*
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
COPY scripts/deploy-init.ts ./scripts/deploy-init.ts
COPY coordinator-source.bundle ./coordinator-source.bundle
RUN chmod -R a+rX /app
RUN node --import tsx --input-type=module -e "import{Context,Effect,Layer}from'effect';import*as S from'@effect/sql-sqlite-node/SqliteClient';await Effect.runPromise(Effect.scoped(Effect.gen(function*(){const c=yield*Layer.build(S.layer({filename:':memory:'}));yield*Context.get(c,S.SqliteClient).unsafe('SELECT 1')})))"
USER node
ENV NODE_ENV=production
CMD ["node", "--import", "tsx", "src/main.ts", "broker"]
