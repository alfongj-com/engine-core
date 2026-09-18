# Use cargo chef for dependency caching
FROM lukemathwalker/cargo-chef:latest-rust-1-trixie AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    libudev-dev libusb-1.0-0-dev pkg-config libssl-dev clang cmake \
    && rm -rf /var/lib/apt/lists/*
COPY rust-toolchain.toml ./
RUN rustup show

# Public registry dependencies only; no private GitHub deploy key is required.

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - this is the caching Docker layer
RUN cargo chef cook --locked --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --locked --release --bin thirdweb-engine

# Runtime stage
FROM debian:trixie-slim AS runtime

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary
COPY --from=builder /app/target/release/thirdweb-engine /app/thirdweb-engine

# Copy configuration files
COPY --from=builder /app/server/configuration /app/configuration

# Create non-root user for security
RUN useradd -r -s /bin/false appuser
RUN chown -R appuser:appuser /app
USER appuser

# Set environment to production
ENV APP_ENVIRONMENT=production

# Expose port (assuming default port 8080, adjust if needed)
EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["./thirdweb-engine"]