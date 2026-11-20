FROM rust:1.99.1-slim-trixie AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
# Cascade Lake and newer scorer CPUs support the x86-64-v3 instruction set.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --jobs 4 \
    && cp target/release/shopping-sessions /shopping-sessions

FROM debian:trixie-slim
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
CMD ["/usr/local/bin/shopping-sessions"]
