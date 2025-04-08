FROM rust:1.81 AS builder

RUN mkdir /build
WORKDIR /build

COPY --link ./Cargo.toml .
COPY --link ./build.rs .
COPY --link ./src ./src
COPY --link ./tests .

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=kdi-cargo-registry \
    --mount=type=cache,target=/usr/local/cargo/git,id=kdi-cargo-git \
    --mount=type=cache,target=target,id=kdi-target \
    cargo build --release --features s3 && cp target/release/kafka-delta-ingest .

FROM debian:12

RUN apt-get update && apt-get -y install \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY --from=builder /build/kafka-delta-ingest ./
ENTRYPOINT ["/build/kafka-delta-ingest"]
