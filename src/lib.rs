#[macro_use]
extern crate lazy_static;

use arrow::datatypes::Schema as ArrowSchema;
use deltalake::{DeltaTableError, DeltaTransactionError, Schema};
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
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

mod deltalake_ext;
mod transforms;

use crate::deltalake_ext::{DeltaParquetWriter, DeltaWriterError};
use crate::transforms::*;

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

    #[error("PartitionOffsets error: {source}")]
    PartitionOffsets {
        #[from]
        source: PartitionOffsetsError,
    },

    #[error("TransformError: {source}")]
    Transform {
        #[from]
        source: TransformError,
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

#[derive(thiserror::Error, Debug)]
pub enum PartitionOffsetsError {
    #[error("A message was handled on partition {partition} but this partition is not tracked")]
    UntrackedTopicPartition { partition: i32 },

    #[error("Topic partition offset list is empty")]
    MissingPartitionOffsets,

    #[error("Kafka error: {source}")]
    Kafka {
        #[from]
        source: KafkaError,
    },
}

struct PartitionOffsets {
    topic: String,
    assigned: HashMap<i32, Option<i64>>,
}

impl PartitionOffsets {
    pub fn new(topic: String) -> Self {
        let assigned = HashMap::new();

        Self { topic, assigned }
    }

    pub fn track_partition_offset(
        &mut self,
        partition: i32,
        offset: i64,
    ) -> Result<(), PartitionOffsetsError> {
        let o = self
            .assigned
            .get_mut(&partition)
            .ok_or(PartitionOffsetsError::UntrackedTopicPartition { partition })?;

        *o = Some(offset);

        Ok(())
    }

    pub fn reset_from_topic_partition_list(&mut self, topic_partition_list: &TopicPartitionList) {
        self.assigned.clear();
        for element in topic_partition_list.elements().iter() {
            let offset = element.offset();
            match offset {
                Offset::Offset(o) => {
                    self.assigned.insert(element.partition(), Some(o));
                }
                _ => {
                    debug!("Read element with offset {:?}", offset);
                    self.assigned.insert(element.partition(), None);
                }
            }
        }
    }

    pub fn clear(&mut self) {
        self.assigned.clear();
    }

    pub fn create_topic_partition_list(&self) -> Result<TopicPartitionList, PartitionOffsetsError> {
        let mut tpl = TopicPartitionList::new();

        for (k, v) in self.assigned.iter() {
            if let Some(o) = v {
                debug!(
                    "Adding partition offset to new TopicPartitionList {}: {}",
                    k, o
                );
                tpl.add_partition_offset(
                    self.topic.as_str(),
                    k.clone(),
                    Offset::Offset(o.clone()),
                )?;
            }
        }

        Ok(tpl)
    }
}

// TODO: This handles resetting partition offsets to scratch,
// but we also need a way to handle file buffers
/// A custom ConsumerContext implementation for updating partition assignments after a rebalance
struct KafkaJsonToDeltaConsumerContext {
    // pub partition_offsets: Arc<Mutex<HashMap<i32, Option<i64>>>>,
    pub partition_offsets: Arc<Mutex<PartitionOffsets>>,
}

impl ClientContext for KafkaJsonToDeltaConsumerContext {}

impl ConsumerContext for KafkaJsonToDeltaConsumerContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        debug!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        debug!("Post rebalance {:?}", rebalance);

        match rebalance {
            Rebalance::Assign(tpl) => {
                info!("Received new partition assignment list");
                match self.partition_offsets.lock() {
                    Ok(mut partition_offsets) => {
                        partition_offsets.reset_from_topic_partition_list(tpl);
                    }
                    Err(e) => {
                        error!("Error locking partition_offsets {:?}", e);
                    }
                }
            }
            Rebalance::Revoke => {
                info!("Partition assignments revoked");
                match self.partition_offsets.lock() {
                    Ok(mut partition_offsets) => {
                        partition_offsets.clear();
                    }
                    Err(e) => {
                        error!("Error locking partition_offsets {:?}", e);
                    }
                }
            }
            Rebalance::Error(e) => {
                warn!(
                    "Unexpected Kafka error in post_rebalance invocation {:?}",
                    e
                );
            }
        }
    }
}

impl KafkaJsonToDeltaConsumerContext {
    fn new(partition_offsets: Arc<Mutex<PartitionOffsets>>) -> Self {
        Self { partition_offsets }
    }
}

type KafkaJsonToDeltaConsumer = StreamConsumer<KafkaJsonToDeltaConsumerContext>;

pub struct KafkaJsonToDelta {
    topic: String,
    table_location: String,
    allowed_latency: u64,
    max_messages_per_batch: usize,
    min_bytes_per_file: usize,
    transforms: HashMap<String, String>,
    consumer: KafkaJsonToDeltaConsumer,
    partition_offsets: Arc<Mutex<PartitionOffsets>>,
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
        transforms: HashMap<String, String>,
    ) -> Self {
        let mut kafka_client_config = ClientConfig::new();

        kafka_client_config
            .set("bootstrap.servers", kafka_brokers.clone())
            .set("group.id", consumer_group_id)
            // No auto-commits. We will commit explicitly after completing Delta transactions
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");

        if let Some(additional) = additional_kafka_settings {
            for (k, v) in additional.iter() {
                kafka_client_config.set(k, v);
            }
        }

        let partition_offsets = Arc::new(Mutex::new(PartitionOffsets::new(topic.clone())));
        let consumer_context = KafkaJsonToDeltaConsumerContext::new(partition_offsets.clone());

        let consumer: KafkaJsonToDeltaConsumer = kafka_client_config
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
            partition_offsets,
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
        let mut json_buffer: Vec<Value> = Vec::new();
        let mut stream = self.consumer.stream();
        let metadata = delta_table.get_metadata()?.clone();
        let schema = metadata.schema.clone();
        let mut delta_writer =
            DeltaParquetWriter::for_table_path_with_schema(delta_table.table_path.clone(), &schema)
                .await?;

        let arrow_schema_ref = Arc::new(<ArrowSchema as From<&Schema>>::from(
            &metadata.clone().schema,
        ));
        let partition_cols = metadata.partition_columns;

        let mut latency_timer = Instant::now();

        let expressions = compile_transforms(&self.transforms)?;

        let transformer = TransformContext::new(expressions);

        // Handle each message on the Kafka topic in a loop.
        while let Some(message) = stream.next().await {
            if let Some(token) = cancellation_token {
                if token.is_cancelled() {
                    return Ok(());
                }
            }

            debug!("Handling Kafka message.");

            match message {
                Ok(m) => {
                    let mut value = match deserialize_message(&m) {
                        Ok(v) => v,
                        Err(KafkaJsonToDeltaError::KafkaMessageNoPayload { .. }) => continue,
                        Err(KafkaJsonToDeltaError::KafkaMessageDeserialization { .. }) => continue,
                        Err(e) => return Err(e),
                    };

                    match transformer.transform(&mut value, &m) {
                        Ok(_) => {
                            debug!("Transformed value {:?}", value);
                        }
                        Err(e) => {
                            // TODO: Add better transform failure handling, ideally send to a dlq
                            error!("Message transform failed {:?}", e);
                            continue;
                        }
                    }

                    // Append to json buffer
                    json_buffer.push(value);

                    // TODO: the rest of this method needs a good cleanup so individual chunks can
                    // be unit tested

                    debug!("Added JSON message to buffer.");

                    {
                        // Track the partition and offset for this message to commit later when flushing
                        // the json buffer
                        let mut partition_offsets = self.partition_offsets.lock().map_err(|e| {
                            KafkaJsonToDeltaError::PoisonedPartitionOffsetMutex {
                                error_string: e.to_string(),
                            }
                        })?;

                        partition_offsets.track_partition_offset(m.partition(), m.offset())?;
                    }

                    //
                    // When the json batch contains max_messages_per_batch messages
                    // or the allowed_latency has elapsed, write a record batch.
                    //

                    let should_complete_record_batch = json_buffer.len()
                        == self.max_messages_per_batch
                        || latency_timer.elapsed().as_secs() >= self.allowed_latency;

                    debug!(
                        "Should complete record batch? {}, json_buffer.len: {}. timer.elapsed: {}",
                        should_complete_record_batch,
                        json_buffer.len(),
                        latency_timer.elapsed().as_secs()
                    );

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
                        let should_complete_file = delta_writer.buffer_len()
                            >= self.min_bytes_per_file
                            || latency_timer.elapsed().as_secs() >= self.allowed_latency;

                        debug!(
                            "Should complete file? {}. delta_writer.buffer_len: {}. timer.elapsed: {}",
                            should_complete_file,
                            delta_writer.buffer_len(),
                            latency_timer.elapsed().as_secs()
                        );

                        if should_complete_file {
                            info!("Completing parquet file write.");

                            let add = delta_writer.complete_file(&partition_cols).await?;
                            let action = deltalake::action::Action::add(add);

                            // Create and commit a delta transaction to add the new data file to table state.
                            let mut tx = delta_table.create_transaction(None);
                            // TODO: Pass an Operation::StreamingUpdate(...) for operation, and a tx action containing the partition offset version
                            let committed_version = tx.commit_with(&[action], None).await?;

                            // Reset the timer to track allowed latency for the next file
                            latency_timer = Instant::now();

                            info!("Comitted Delta version {}", committed_version);
                        }

                        // TODO: Kafka partition offsets shall eventually be written to a separate write-ahead-log
                        // to maintain consistency with the data written in each version of the delta log.
                        // The offset commit to Kafka shall be specifically for consumer lag monitoring support.

                        // Commit all assigned partition offsets to Kafka so consumer lag monitors know where we are.

                        let partition_offsets = self.partition_offsets.lock().map_err(|e| {
                            KafkaJsonToDeltaError::PoisonedPartitionOffsetMutex {
                                error_string: e.to_string(),
                            }
                        })?;

                        let topic_partition_list =
                            partition_offsets.create_topic_partition_list()?;

                        info!("Committing offsets");

                        let _ = self
                            .consumer
                            .commit(&topic_partition_list, CommitMode::Async)?;
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
