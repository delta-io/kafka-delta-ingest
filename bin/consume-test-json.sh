#!/bin/bash

RUST_LOG=info ./target/debug/kafka-delta-ingest ingest web_requests ./tests/data/web_requests --allowed_latency 10 -t 'date: substr(meta.producer.timestamp, `0`, `10`)' 'meta.kafka.offset: kafka.offset' 'meta.kafka.partition: kafka.partition' 'meta.kafka.timestamp: kafka.timestamp'
