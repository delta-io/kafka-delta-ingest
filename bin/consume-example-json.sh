#!/bin/bash

export AWS_ENDPOINT_URL=http://0.0.0.0:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

RUST_LOG=debug ./target/debug/kafka-delta-ingest ingest web_requests ./tests/data/web_requests \
  -l 60 \
  -a web_requests \
  -K "auto.offset.reset=earliest" \
  -t 'date: substr(meta.producer.timestamp, `0`, `10`)' \
      'meta.kafka.offset: kafka.offset' \
      'meta.kafka.partition: kafka.partition' \
      'meta.kafka.topic: kafka.topic' \
  -s "localhost:8125"


