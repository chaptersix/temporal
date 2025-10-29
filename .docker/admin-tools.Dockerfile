ARG ALPINE_IMAGE=alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412

FROM ${ALPINE_IMAGE}

ARG TARGETARCH
ARG TEMPORAL_SHA=unknown
ARG TEMPORAL_VERSION=dev

LABEL org.opencontainers.image.title="temporal-admin-tools" \
    org.opencontainers.image.description="Temporal Admin Tools" \
    org.opencontainers.image.url="https://github.com/temporalio/temporal" \
    org.opencontainers.image.source="https://github.com/temporalio/temporal" \
    org.opencontainers.image.licenses="MIT" \
    org.opencontainers.image.revision="${TEMPORAL_SHA}" \
    org.opencontainers.image.version="${TEMPORAL_VERSION}"

# Install runtime dependencies (tini is pinned to alpine package version)
RUN apk upgrade --no-cache && \
    apk add --no-cache \
    ca-certificates \
    tzdata \
    tini

# Setup temporal user
RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

# Copy binaries
COPY --chmod=755 ./.docker/dist/${TARGETARCH}/temporal /usr/local/bin/
COPY --chmod=755 ./.docker/dist/${TARGETARCH}/tdbg /usr/local/bin/
COPY --chmod=755 ./.docker/dist/${TARGETARCH}/temporal-cassandra-tool /usr/local/bin/
COPY --chmod=755 ./.docker/dist/${TARGETARCH}/temporal-sql-tool /usr/local/bin/
COPY --chmod=755 ./.docker/dist/${TARGETARCH}/temporal-elasticsearch-tool /usr/local/bin/
COPY ./schema /etc/temporal/schema

USER temporal
WORKDIR /etc/temporal

ENTRYPOINT ["tini", "--", "sleep", "infinity"]
