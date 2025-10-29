# Flat Dockerfile for temporalio/server
# Combines base-server and server layers into a single flat Dockerfile with pinned digests

ARG ALPINE_IMAGE=alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412
ARG GOLANG_IMAGE=golang:1.24-alpine3.22@sha256:8f8959f38530d159bf71d0b3eb0c547dc61e7959d8225d1599cf762477384923

# Install dockerize in builder stage
FROM ${GOLANG_IMAGE} AS builder
ARG DOCKERIZE_VERSION=v0.9.2
RUN go install github.com/jwilder/dockerize@${DOCKERIZE_VERSION}
RUN cp $(which dockerize) /usr/local/bin/dockerize

# Main server image
FROM ${ALPINE_IMAGE}
ARG TARGETARCH
ARG TEMPORAL_SHA=unknown
ARG TCTL_SHA=unknown
ARG TEMPORAL_VERSION=dev



# Install base packages
RUN apk upgrade --no-cache && \
    apk add --no-cache \
    ca-certificates \
    tzdata \
    bash \
    curl

# Copy dockerize from builder
COPY --from=builder /usr/local/bin/dockerize /usr/local/bin

# Set shell to bash
SHELL ["/bin/bash", "-c"]

# Set up Temporal environment and user
WORKDIR /etc/temporal
ENV TEMPORAL_HOME=/etc/temporal
EXPOSE 6933 6934 6935 6939 7233 7234 7235 7239

# Create temporal user and group
RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal && \
    mkdir -p /etc/temporal/config && \
    chown -R temporal:temporal /etc/temporal/config

# Store component versions in the environment
ENV TEMPORAL_SHA=${TEMPORAL_SHA}
ENV TCTL_SHA=${TCTL_SHA}

# Copy binaries
COPY --chmod=755 ./build/${TARGETARCH}/tctl /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/tctl-authorization-plugin /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-server /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal /usr/local/bin/

# Copy configs
COPY ./temporal/config/dynamicconfig/docker.yaml /etc/temporal/config/dynamicconfig/docker.yaml
COPY ./temporal/docker/config_template.yaml /etc/temporal/config/config_template.yaml

# Copy scripts
COPY --chmod=755 ./docker/entrypoint.sh /etc/temporal/entrypoint.sh
COPY --chmod=755 ./docker/start-temporal.sh /etc/temporal/start-temporal.sh

USER temporal

ENTRYPOINT ["/etc/temporal/entrypoint.sh"]
