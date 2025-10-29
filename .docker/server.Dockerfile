ARG ALPINE_IMAGE=alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412

FROM ${ALPINE_IMAGE}

ARG TARGETARCH
ARG TEMPORAL_SHA=unknown
ARG DOCKERIZE_VERSION=v0.9.2
ARG TEMPORAL_VERSION=dev

# Install runtime dependencies and dockerize
RUN apk upgrade --no-cache && \
    apk add --no-cache \
    ca-certificates \
    tzdata \
    curl \
    go && \
    go install github.com/jwilder/dockerize@${DOCKERIZE_VERSION} && \
    mv /root/go/bin/dockerize /usr/local/bin/dockerize && \
    apk del go && \
    rm -rf /root/go

# Setup temporal user and directories
RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal && \
    mkdir -p /etc/temporal/config && \
    chown -R temporal:temporal /etc/temporal/config

WORKDIR /etc/temporal

ENV TEMPORAL_HOME=/etc/temporal
ENV TEMPORAL_SHA=${TEMPORAL_SHA}
EXPOSE 6933 6934 6935 6939 7233 7234 7235 7239

# Copy binaries
COPY --chmod=755 ./.docker/dist/${TARGETARCH}/temporal-server /usr/local/bin/

# Copy configs
COPY ./config/dynamicconfig/docker.yaml /etc/temporal/config/dynamicconfig/docker.yaml
COPY ./docker/config_template.yaml /etc/temporal/config/config_template.yaml

# Copy scripts
COPY --chmod=755 ./.docker/scripts/entrypoint.sh /etc/temporal/entrypoint.sh
COPY --chmod=755 ./.docker/scripts/start-temporal.sh /etc/temporal/start-temporal.sh

USER temporal

ENTRYPOINT ["/etc/temporal/entrypoint.sh"]
