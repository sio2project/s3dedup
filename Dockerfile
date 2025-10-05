# Build stage
FROM rustlang/rust:nightly-slim as builder

WORKDIR /usr/src/s3dedup

# Install build dependencies for vendored OpenSSL
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    perl \
    make \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY tests ./tests

# Build the application in release mode
# Docker buildx uses QEMU to emulate the target architecture
RUN cargo build --release

# Runtime stage
FROM debian:trixie-slim

WORKDIR /app

# Install runtime dependencies including curl for health checks
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /usr/src/s3dedup/target/release/s3dedup /usr/local/bin/s3dedup

# Create directory for config and database
RUN mkdir -p /app/data

# Set the working directory where config.json should be placed
WORKDIR /app

# Expose default port (can be overridden in config)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the server command with environment variables by default
ENTRYPOINT ["s3dedup"]
CMD ["server", "--env"]
