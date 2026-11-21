FROM rust:1.99.1-slim-trixie AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
# Cascade Lake and newer scorer CPUs support the x86-64-v3 instruction set.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo clean --release -p shopping-sessions \
    && cargo build --release --locked --jobs 4 \
    && cp target/release/shopping-sessions /shopping-sessions

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
ENV MALLOC_CONF=thp:always,metadata_thp:auto,dirty_decay_ms:-1,muzzy_decay_ms:-1
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
CMD ["/usr/local/bin/shopping-sessions"]
