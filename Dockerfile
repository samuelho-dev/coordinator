FROM golang:1.25-bookworm AS go
FROM node:24.21.0-bookworm-slim AS build
COPY --from=go /usr/local/go /usr/local/go
COPY --from=go /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENV PATH=/usr/local/go/bin:$PATH
RUN npm install --global pnpm@11.9.0
WORKDIR /build
COPY . .
ARG COORDINATOR_IMAGE_SHA
RUN pnpm install --frozen-lockfile && PATH=/build/node_modules/.bin:$PATH COORDINATOR_BUILD_TARGETS=linux-x64 go run ./cmd/build

FROM scratch
LABEL org.opencontainers.image.licenses="MIT"
ARG COORDINATOR_IMAGE_SHA
ENV COORDINATOR_IMAGE_SHA=${COORDINATOR_IMAGE_SHA}
LABEL org.opencontainers.image.revision=${COORDINATOR_IMAGE_SHA}
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /build/dist/bin/linux-x64/coordinator /usr/local/bin/coordinator
COPY --from=build /build/LICENSE /usr/share/licenses/coordinator/
USER 1000:1000
ENTRYPOINT ["coordinator"]
CMD ["inspector"]
