extern crate anyhow;

use arrow::datatypes::Schema as ArrowSchema;
use deltalake::{DeltaTableError, DeltaTransactionError, Schema};
use futures::stream::StreamExt;
use log::{debug, error, info, warn};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    Message,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::deltalake_ext;
use crate::deltalake_ext::{DeltaParquetWriter, DeltaWriterError};

#[derive(thiserror::Error, Debug)]
pub enum KafkaJsonToDeltaError {
    #[error("Failed to start stream")]
    StreamStartFailed,

    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("DeltaTransaction failed: {source}")]
    DeltaTransaction {
        #[from]
        source: DeltaTransactionError,
    },

    #[error("DeltaWriter error: {source}")]
    DeltaWriter {
        #[from]
        source: DeltaWriterError,
    },
}

pub struct KafkaJsonToDelta {
    topic: String,
    table_location: String,
    allowed_latency: u64,
    max_messages_per_batch: usize,
    min_bytes_per_file: usize,
    consumer: StreamConsumer,
}

/// Encapsulates a single topic-to-table ingestion stream.
impl KafkaJsonToDelta {
    /// Creates a new instance of KafkaJsonToDelta.
    pub fn new(
        topic: String,
        table_location: String,
        kafka_brokers: String,
        consumer_group_id: String,
        additional_kafka_settings: Option<HashMap<String, String>>,
        allowed_latency: u64,
        max_messages_per_batch: usize,
        min_bytes_per_file: usize,
    ) -> Self {
        let mut kafka_client_config = ClientConfig::new();

        kafka_client_config
            .set("bootstrap.servers", kafka_brokers.clone())
            .set("group.id", consumer_group_id)
            // Commit every 5 seconds... but...
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            // but... only commit the offsets explicitly stored via `consumer.store_offset`.
            .set("enable.auto.offset.store", "false");

        if let Some(additional) = additional_kafka_settings {
            for (k, v) in additional.iter() {
                kafka_client_config.set(k, v);
            }
        }

        let consumer: StreamConsumer = kafka_client_config
            .create()
            .expect("Failed to create StreamConsumer");

        Self {
            topic,
            table_location,
            allowed_latency,
            max_messages_per_batch,
            min_bytes_per_file,
            consumer,
        }
    }

    /// Starts the topic-to-table ingestion stream.
    pub async fn start(&mut self) -> Result<(), KafkaJsonToDeltaError> {
        self.consumer
            .subscribe(&[self.topic.as_str()])
            .expect("Consumer subscription failed");

        start(
            &mut self.consumer,
            self.table_location.as_str(),
            self.allowed_latency,
            self.max_messages_per_batch,
            self.min_bytes_per_file,
        ).await
    }
}

async fn start(
    consumer: &mut StreamConsumer,
    delta_table_path: &str,
    allowed_latency: u64,
    max_messages_per_batch: usize,
    min_bytes_per_file: usize,
) -> Result<(), KafkaJsonToDeltaError> {
    let mut delta_table = deltalake::open_table(delta_table_path).await?;
    let mut json_buffer: Vec<Value> = Vec::new();
    let mut stream = consumer.stream();
    let metadata = delta_table.get_metadata()?.clone();
    let schema = metadata.schema.clone();
    let mut delta_writer =
        DeltaParquetWriter::for_table_path_with_schema(delta_table.table_path.clone(), &schema)
            .await?;

    let arrow_schema_ref = Arc::new(<ArrowSchema as From<&Schema>>::from(
        &metadata.clone().schema,
    ));
    let partition_cols = metadata.partition_columns;

    let mut timer = Instant::now();

    // Handle each message on the Kafka topic in a loop.
    while let Some(message) = stream.next().await {
        match message {
            Ok(m) => {
                debug!("Handling Kafka message.");

                // Deserialize the rdkafka message into a serde_json::Value
                let message_bytes = match m.payload() {
                    Some(bytes) => bytes,
                    None => {
                        warn!(
                            "Payload has no bytes at partition: {} with offset: {}",
                            m.partition(),
                            m.offset()
                        );
                        continue;
                    }
                };

                let mut value: Value = match serde_json::from_slice(message_bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        // TODO: Add better deserialization error handling
                        // Ideally, provide an option to send the message bytes to a dead letter queue
                        error!("Error deserializing message {:?}", e);
                        continue;
                    }
                };

                // Transform the message according to topic configuration.
                // This is a noop for now.
                let _ = transform_message(&mut value, &m);

                // Append to json batch
                json_buffer.push(value);

                //
                // When the json batch contains max_messages_per_batch messages
                // or the allowed_latency has elapsed, write a record batch.
                //

                let should_complete_record_batch = json_buffer.len() == max_messages_per_batch
                    || timer.elapsed().as_secs() >= allowed_latency;

                if should_complete_record_batch {
                    info!("Creating Arrow RecordBatch from JSON message buffer.");

                    // Convert the json buffer to an arrow record batch.
                    let record_batch = deltalake_ext::record_batch_from_json_buffer(
                        arrow_schema_ref.clone(),
                        json_buffer.as_slice(),
                    )?;

                    // Clear the json buffer so we can start clean on the next round.
                    json_buffer.clear();

                    // Write the arrow record batch to our delta writer that wraps an in memory cursor for tracking bytes.
                    delta_writer
                        .write_record_batch(&partition_cols, &record_batch)
                        .await?;

                    // When the memory buffer meets min bytes per file or allowed latency is met, complete the file and start a new one.
                    let should_complete_file = delta_writer.buffer_len() >= min_bytes_per_file
                        || timer.elapsed().as_secs() >= allowed_latency;

                    if should_complete_file {
                        info!("Completing parquet file write.");

                        let add = delta_writer.complete_file(&partition_cols).await?;
                        let action = deltalake::action::Action::add(add);

                        // Create and commit a delta transaction to add the new data file to table state.
                        let mut tx = delta_table.create_transaction(None);
                        // TODO: Pass an Operation::StreamingUpdate(...) for operation, and a tx action containing the partition offset version
                        let committed_version = tx.commit_with(&[action], None).await?;

                        // Reset the timer to track allowed latency for the next file
                        timer = Instant::now();

                        info!("Comitted Delta version {}", committed_version);
                    }

                    // TODO: Kafka partition offsets shall eventually be written to a separate write-ahead-log
                    // to maintain consistency with the data written in each version of the delta log.
                    // The offset commit to Kafka shall be specifically for consumer lag monitoring support.

                    // Commit the offset to Kafka so consumer lag monitors know where we are.
                    if let Err(e) = consumer.store_offset(&m) {
                        warn!("Failed to commit consumer offsets {:?}", e);
                    } else {
                        info!("Committed offset {}", m.offset());
                    }
                }
            }
            Err(e) => {
                // TODO: What does this error really imply? Determine if this should stop the stream.
                error!(
                    "Error getting BorrowedMessage while processing stream {:?}",
                    e
                );
            }
        }
    }

    Ok(())
}

fn transform_message<M>(_value: &mut Value, _message: &M) -> Result<(), KafkaJsonToDeltaError>
where
    M: Message,
{
    // TODO: transform the message according to topic configuration options
    // ...

    Ok(())
}
