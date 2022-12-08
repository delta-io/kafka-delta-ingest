FROM rust:1.60 AS builder

WORKDIR /build

COPY ./ .

RUN cargo build --release

FROM ubuntu

RUN apt-get update
RUN apt-get install -y ca-certificates

WORKDIR /build

COPY --from=builder /build/target/release/kafka-delta-ingest ./
ENTRYPOINT ["/build/kafka-delta-ingest"]