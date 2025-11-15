ARG RELEASE=0fb6c47127baa16f95e49af622be07ba931aa0e4 # 0.1.31
FROM rust:latest AS builder
WORKDIR /app

# Install build dependencies for compiling native code to wasm
RUN apt-get update && apt-get install -y \
    clang \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/XiangpengHao/parquet-viewer && \
    cd parquet-viewer && \
    git checkout ${RELEASE} && \
    rm -rf .git

WORKDIR /app/parquet-viewer

# Install specific nightly version to match nix flake
RUN rustup toolchain install nightly-2025-08-28 && \
    rustup default nightly-2025-08-28 && \
    rustup target add wasm32-unknown-unknown --toolchain nightly-2025-08-28 && \
    cargo +nightly-2025-08-28 install trunk && \
    trunk build --release --locked

# Stage 2: Create the final, smaller image with a web server
FROM nginx:alpine
WORKDIR /usr/share/nginx/html
COPY --from=builder /app/parquet-viewer/dist/ .
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]