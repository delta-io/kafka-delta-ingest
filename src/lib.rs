#[macro_use]
extern crate lazy_static;

use arrow::datatypes::Schema as ArrowSchema;
use deltalake::{action, DeltaTableError, DeltaTransactionError, Schema};
use futures::stream::StreamExt;
use log::{debug, error, info, warn};
use rdkafka::{
    client::ClientContext,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaError,
    Message, Offset, TopicPartitionList,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    mpsc::{Receiver, Sender},
    Arc, Mutex,
};
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod consumer;
mod deltalake_ext;
mod message_buffers;
mod transforms;
mod write_ahead_log;

use crate::consumer::*;
use crate::deltalake_ext::{DeltaParquetWriter, DeltaWriterError};
use crate::message_buffers::*;
use crate::transforms::*;
use crate::write_ahead_log::*;

type DataTypeTransactionId = i64;
type DataTypePartition = i32;
type DataTypeOffset = i64;

#[derive(thiserror::Error, Debug)]
pub enum KafkaJsonToDeltaError {
    #[error("Kafka error: {source}")]
    Kafka {
        #[from]
        source: KafkaError,
    },

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

    #[error("TransformError: {source}")]
    Transform {
        #[from]
        source: TransformError,
    },

    #[error("WriteAheadLogError: {source}")]
    WriteAheadLog {
        #[from]
        source: WriteAheadLogError,
    },

    #[error("Failed to start stream")]
    StreamStartFailed,

    #[error("Partition offset mutex is poisoned: {error_string}")]
    PoisonedPartitionOffsetMutex { error_string: String },

    #[error("A message was handled on partition {partition} but this partition is not tracked")]
    UntrackedTopicPartition { partition: i32 },

    #[error("Topic partition offset list is empty")]
    MissingPartitionOffsets,

    #[error("Kafka message payload was empty at partition {partition} offset {offset}")]
    KafkaMessageNoPayload { partition: i32, offset: i64 },

    #[error("Kafka message deserialization failed at partition {partition} offset {offset}")]
    KafkaMessageDeserialization { partition: i32, offset: i64 },
}

/// Encapsulates a single topic-to-table ingestion stream.
pub struct KafkaJsonToDelta {
    topic: String,
    table_location: String,
    allowed_latency: u64,
    max_messages_per_batch: usize,
    min_bytes_per_file: usize,
    transforms: HashMap<String, String>,
    consumer: StreamConsumer<consumer::Context>,
    partition_assignment: Arc<Mutex<PartitionAssignment>>,
    consumer_id: Uuid,
}

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
        transforms: HashMap<String, String>,
    ) -> Self {
        let consumer_id = Uuid::new_v4();
        let mut kafka_client_config = ClientConfig::new();

        kafka_client_config
            .set("bootstrap.servers", kafka_brokers.clone())
            .set("group.id", consumer_group_id)
            .set("enable.auto.commit", "true");

        if let Some(additional) = additional_kafka_settings {
            for (k, v) in additional.iter() {
                kafka_client_config.set(k, v);
            }
        }

        let partition_assignment = Arc::new(Mutex::new(PartitionAssignment::new()));
        let consumer_context = consumer::Context::new(partition_assignment.clone());

        let consumer: StreamConsumer<consumer::Context> = kafka_client_config
            .create_with_context(consumer_context)
            .expect("Failed to create StreamConsumer");

        Self {
            topic,
            table_location,
            allowed_latency,
            max_messages_per_batch,
            min_bytes_per_file,
            transforms,
            consumer,
            partition_assignment,
            consumer_id,
        }
    }

    /// Starts the topic-to-table ingestion stream.
    pub async fn start(
        &mut self,
        cancellation_token: Option<&CancellationToken>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        info!("Starting stream");

        self.consumer
            .subscribe(&[self.topic.as_str()])
            .expect("Consumer subscription failed");

        let res = self.run_loop(cancellation_token).await;

        if res.is_err() {
            error!("Stream stopped with error result: {:?}", res);
        } else {
            info!("Stream stopped");
        }

        res
    }

    async fn run_loop(
        &mut self,
        cancellation_token: Option<&CancellationToken>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        let mut delta_table = deltalake::open_table(self.table_location.as_str()).await?;
        let metadata = delta_table.get_metadata()?.clone();
        let schema = metadata.schema.clone();
        let arrow_schema_ref = Arc::new(<ArrowSchema as From<&Schema>>::from(
            &metadata.clone().schema,
        ));
        let partition_columns = metadata.partition_columns;

        let mut delta_writer =
            DeltaParquetWriter::for_table_path_with_schema(delta_table.table_path.clone(), &schema)
                .await?;

        let transformer = Transformer::from_transforms(&self.transforms)?;

        let mut value_buffers = ValueBuffers::new();

        let wal = WriteAheadLog::new(self.consumer_id.to_string());
        let previous_wal_entry = wal.get_last_completed_entry()?;

        if let Some(entry) = previous_wal_entry {
            self.init_offsets(&entry.partition_offsets)?;
        }

        let mut stream = self.consumer.stream();

        let mut latency_timer = Instant::now();

        // Handle each message on the Kafka topic
        while let Some(message) = stream.next().await {
            // Exit if the cancellation token is set
            if let Some(token) = cancellation_token {
                if token.is_cancelled() {
                    return Ok(());
                }
            }

            match message {
                Ok(m) => {
                    let mut value = match deserialize_message(&m) {
                        Ok(v) => v,
                        Err(KafkaJsonToDeltaError::KafkaMessageNoPayload { .. }) => continue,
                        Err(KafkaJsonToDeltaError::KafkaMessageDeserialization { .. }) => continue,
                        Err(e) => return Err(e),
                    };

                    match transformer.transform(&mut value, &m) {
                        Err(e) => {
                            // TODO: Add better transform failure handling, ideally send to a dlq
                            error!("Message transform failed {:?}", e);
                            continue;
                        }
                        _ => {}
                    }

                    value_buffers.add(m.partition(), m.offset(), value);

                    if self.should_complete_record_batch(&value_buffers, &latency_timer) {
                        info!("Creating Arrow RecordBatch.");

                        let (values, partition_offsets) = consume_and_track(
                            self.partition_assignment.clone(),
                            &mut value_buffers,
                        )?;

                        let record_batch = deltalake_ext::record_batch_from_json(
                            arrow_schema_ref.clone(),
                            values.as_slice(),
                        )?;

                        delta_writer
                            .write_record_batch(&partition_columns, &record_batch)
                            .await?;

                        if self.should_complete_file(&delta_writer, &latency_timer) {
                            info!("Writing parquet file");

                            let add = delta_writer.complete_file(&partition_columns).await?;
                            let action = action::Action::add(add);

                            let wal_entry = wal.prepare_entry(&partition_offsets)?;

                            // Include a Delta tx action containing our WAL transaction id
                            let txn = action::Action::txn(action::Txn {
                                appId: "".to_string(),
                                version: wal_entry.transaction_id,
                                lastUpdated: 0,
                            });

                            let mut tx = delta_table.create_transaction(None);
                            let committed_version = tx.commit_with(&[txn, action], None).await;

                            if let Ok(version) = committed_version {
                                info!("Comitted Delta version {}", version);

                                wal.complete_entry(wal_entry.transaction_id, version)?;
                            } else {
                                wal.fail_entry(wal_entry.transaction_id)?;
                                break;
                            }

                            // Reset the latency timer to track allowed latency for the next file
                            latency_timer = Instant::now();
                        }
                    }
                }
                Err(e) => {
                    // TODO: What does an error unwrapping the BorrowedMessage mean? Determine if this should stop the stream.
                    error!(
                        "Error getting BorrowedMessage while processing stream {:?}",
                        e
                    );
                }
            }
        }

        Ok(())
    }

    fn should_complete_record_batch(
        &self,
        value_buffers: &ValueBuffers,
        latency_timer: &Instant,
    ) -> bool {
        value_buffers.len() == self.max_messages_per_batch
            || latency_timer.elapsed().as_secs() >= self.allowed_latency
    }

    fn should_complete_file(
        &self,
        delta_writer: &DeltaParquetWriter,
        latency_timer: &Instant,
    ) -> bool {
        delta_writer.buffer_len() >= self.min_bytes_per_file
            || latency_timer.elapsed().as_secs() >= self.allowed_latency
    }

    fn init_offsets(
        &mut self,
        offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    ) -> Result<(), KafkaJsonToDeltaError> {
        let topic_partition_list = self.consumer.assignment()?;
        let mut partition_assignment = self.partition_assignment.lock().map_err(|e| {
            KafkaJsonToDeltaError::PoisonedPartitionOffsetMutex {
                error_string: e.to_string(),
            }
        })?;

        partition_assignment.reset_from_topic_partition_list(&topic_partition_list);
        partition_assignment.update_offsets(offsets);

        for (k, v) in partition_assignment.partition_offsets().iter() {
            self.consumer
                .seek(self.topic.as_str(), *k, Offset::Offset(*v), None)?;
        }

        Ok(())
    }

    fn assigned_partitions(&self) -> Result<Vec<DataTypePartition>, KafkaJsonToDeltaError> {
        let partition_assignment = self.partition_assignment.lock().map_err(|e| {
            KafkaJsonToDeltaError::PoisonedPartitionOffsetMutex {
                error_string: e.to_string(),
            }
        })?;

        let partitions = partition_assignment.partitions();

        Ok(partitions)
    }

    fn consume_and_track(
        &mut self,
        buffers: &mut ValueBuffers,
    ) -> Result<Vec<Value>, KafkaJsonToDeltaError> {
        let mut partition_assignment = self.partition_assignment.lock().map_err(|e| {
            KafkaJsonToDeltaError::PoisonedPartitionOffsetMutex {
                error_string: e.to_string(),
            }
        })?;

        let partitions = partition_assignment.partitions();

        let ConsumedBuffers {
            values,
            partition_offsets,
        } = buffers.consume_or_drop_partitions(partitions.as_slice());

        partition_assignment.update_offsets(&partition_offsets);

        Ok(values)
    }
}

fn deserialize_message<M>(m: &M) -> Result<serde_json::Value, KafkaJsonToDeltaError>
where
    M: Message,
{
    // Deserialize the rdkafka message into a serde_json::Value
    let message_bytes = match m.payload() {
        Some(bytes) => bytes,
        None => {
            warn!(
                "Payload has no bytes at partition: {} with offset: {}",
                m.partition(),
                m.offset()
            );
            return Err(KafkaJsonToDeltaError::KafkaMessageNoPayload {
                partition: m.partition(),
                offset: m.offset(),
            });
        }
    };

    let value: Value = match serde_json::from_slice(message_bytes) {
        Ok(v) => v,
        Err(e) => {
            // TODO: Add better deserialization error handling
            // Ideally, provide an option to send the message bytes to a dead letter queue
            error!("Error deserializing message {:?}", e);
            return Err(KafkaJsonToDeltaError::KafkaMessageDeserialization {
                partition: m.partition(),
                offset: m.offset(),
            });
        }
    };

    Ok(value)
}

fn consume_and_track(
    partition_assignment: Arc<Mutex<PartitionAssignment>>,
    buffers: &mut ValueBuffers,
) -> Result<(Vec<Value>, HashMap<DataTypePartition, DataTypeOffset>), KafkaJsonToDeltaError> {
    let mut partition_assignment = partition_assignment.lock().map_err(|e| {
        KafkaJsonToDeltaError::PoisonedPartitionOffsetMutex {
            error_string: e.to_string(),
        }
    })?;

    let partitions = partition_assignment.partitions();

    let ConsumedBuffers {
        values,
        partition_offsets,
    } = buffers.consume_or_drop_partitions(partitions.as_slice());

    partition_assignment.update_offsets(&partition_offsets);

    let partition_offsets = partition_assignment.partition_offsets();

    Ok((values, partition_offsets))
}
