FROM node:24.21.0-bookworm-slim AS build
RUN npm install --global pnpm@11.9.0
WORKDIR /build
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json tsconfig.build.json ./
COPY src ./src
COPY scripts/install.ts scripts/smoke.ts scripts/deployment-smoke.ts ./scripts/
RUN pnpm install --frozen-lockfile && pnpm run build

FROM node:24.21.0-bookworm-slim AS production-dependencies
RUN npm install --global pnpm@11.9.0
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --prod --frozen-lockfile

FROM node:24.21.0-bookworm-slim
LABEL org.opencontainers.image.licenses="MIT"
ARG COORDINATOR_IMAGE_SHA
ENV COORDINATOR_IMAGE_SHA=${COORDINATOR_IMAGE_SHA}
LABEL org.opencontainers.image.revision=${COORDINATOR_IMAGE_SHA}
WORKDIR /app
COPY --chown=node:node --from=production-dependencies /app/node_modules ./node_modules
COPY --chown=node:node package.json ./
COPY --chown=node:node --from=build /build/dist ./dist
COPY --chown=node:node LICENSE THIRD_PARTY_NOTICES.md ./
USER node
ENV NODE_ENV=production
CMD ["node", "dist/src/main.js", "inspector"]
