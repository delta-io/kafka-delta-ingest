#!/bin/bash

set -e

KAFKA_BROKER=${KAFKA_BROKER:-"localhost:9092"}
TOPIC=${TOPIC:-"web_requests"}
DATA_FILE=${DATA_FILE:-"/data/web_requests.json"}

# Untar the data if it's compressed
if [[ $DATA_FILE == *.tar.gz ]]; then
    echo "Extracting tarball..."
    tar -xzvf $DATA_FILE -C /tmp
    DATA_FILE="/tmp/$(basename $DATA_FILE .tar.gz)"
fi

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
until kafkacat -b $KAFKA_BROKER -L &>/dev/null; do
  sleep 1
done
echo "Kafka is ready"

# Create topic if it doesn't exist
kafkacat -b $KAFKA_BROKER -t $TOPIC -C -e

# Upload data to Kafka
echo "Uploading data to Kafka..."
cat $DATA_FILE | kafkacat -b $KAFKA_BROKER -t $TOPIC -P

echo "Data upload complete"