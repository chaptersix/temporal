# Flat Dockerfile for temporalio/admin-tools
# Combines base-admin-tools and admin-tools layers into a single flat Dockerfile with pinned digests

ARG ALPINE_IMAGE=alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412

# Install cqlsh in builder stage
FROM ${ALPINE_IMAGE} AS cqlsh-builder
RUN apk add --update --no-cache \
    python3-dev \
    musl-dev \
    libev-dev \
    gcc \
    pipx && \
    pipx install --global cqlsh

# Main admin tools image
FROM ${ALPINE_IMAGE}
ARG TARGETARCH
ARG TEMPORAL_SHA=unknown
ARG TCTL_SHA=unknown
ARG TEMPORAL_VERSION=dev



# Install base packages and admin tools dependencies
RUN apk upgrade --no-cache && \
    apk add --no-cache \
    python3 \
    libev \
    ca-certificates \
    tzdata \
    bash \
    curl \
    jq \
    yq \
    mysql-client \
    postgresql-client \
    expat \
    tini \
    bash-completion

# Copy cqlsh from cqlsh-builder
COPY --from=cqlsh-builder /opt/pipx/venvs/cqlsh /opt/pipx/venvs/cqlsh
RUN ln -s /opt/pipx/venvs/cqlsh/bin/cqlsh /usr/local/bin/cqlsh

# Validate cqlsh installation
RUN cqlsh --version

# Set shell to bash
SHELL ["/bin/bash", "-c"]

# Create temporal user and group
RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

# Copy all admin tool binaries
COPY --chmod=755 ./build/${TARGETARCH}/tctl /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/tctl-authorization-plugin /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-cassandra-tool /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-sql-tool /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/tdbg /usr/local/bin/

# Copy schema files
COPY ./temporal/schema /etc/temporal/schema

# Set up bash completion for temporal CLI
RUN temporal completion bash > /etc/bash/temporal-completion.sh

USER temporal
WORKDIR /etc/temporal

# Keep the container running
ENTRYPOINT ["tini", "--", "sleep", "infinity"]
