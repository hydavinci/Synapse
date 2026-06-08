# ============================================================================
# Synapse Memory Protocol Server — Multi-Stage Docker Build
# ============================================================================
# Build: docker build -t synapse-server .
# Run:   docker run -p 9090:9090 synapse-server
# ============================================================================

# Stage 1: Build the Rust server binary
FROM rust:1.79-bookworm AS builder

# Install protobuf compiler for gRPC codegen
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/synapse

# Cache dependency builds — copy manifests first
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY proto/ proto/

# Build dependencies first (cache layer)
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/synapse/target \
    cargo build --release --bin synapse-server 2>/dev/null || true

# Copy full source and build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/synapse/target \
    cargo build --release --bin synapse-server && \
    cp target/release/synapse-server /usr/local/bin/synapse-server

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r synapse && useradd -r -g synapse -d /opt/synapse -s /sbin/nologin synapse

WORKDIR /opt/synapse

# Copy binary from builder
COPY --from=builder /usr/local/bin/synapse-server /usr/local/bin/synapse-server

# Copy default configuration
COPY config/ /opt/synapse/config/

# Create data directory
RUN mkdir -p /opt/synapse/data && chown -R synapse:synapse /opt/synapse

# Switch to non-root user
USER synapse

# Expose gRPC and HTTP ports
EXPOSE 9090 9091

# Health check via gRPC health endpoint
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:9091/health || exit 1

# Environment defaults
ENV SYNAPSE_CONFIG_PATH=/opt/synapse/config/default.toml \
    SYNAPSE_DATA_DIR=/opt/synapse/data \
    SYNAPSE_LOG_LEVEL=info \
    RUST_BACKTRACE=1

ENTRYPOINT ["synapse-server"]
CMD ["--config", "/opt/synapse/config/default.toml"]
