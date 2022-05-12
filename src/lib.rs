//! Implementations supporting the kafka-delta-ingest daemon

//! ## Feature flags
//!
//! - `dynamic-linking`: Use the `dynamic-linking` feature of the `rdkafka` crate and link to the system's version of librdkafka instead of letting the `rdkafka` crate builds its own librdkafka.

#![deny(warnings)]
#![deny(missing_docs)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate strum_macros;

#[cfg(test)]
extern crate serde_json;

use coercions::CoercionTree;
use deltalake::{DeltaDataTypeVersion, DeltaTable, DeltaTableError};
use futures::stream::StreamExt;
use log::{debug, error, info, warn};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaError,
    util::Timeout,
    ClientContext, Message, Offset, TopicPartitionList,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

mod coercions;
mod dead_letters;
mod delta_helpers;
mod metrics;
mod offsets;
mod transforms;
mod value_buffers;
pub mod writer;

use crate::offsets::WriteOffsetsError;
use crate::value_buffers::{ConsumedBuffers, ValueBuffers};
use crate::{
    dead_letters::*,
    metrics::*,
    transforms::*,
    writer::{DataWriter, DataWriterError},
};
use delta_helpers::*;
use deltalake::checkpoints::CheckpointError;
use deltalake::storage::StorageError;
use dynamodb_lock::DynamoError;
use std::ops::Add;

/// Type alias for Kafka partition
pub type DataTypePartition = i32;
/// Type alias for Kafka message offset
pub type DataTypeOffset = i64;

/// The default number of times to retry a delta commit when optimistic concurrency fails.
pub(crate) const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS: u32 = 10_000_000;

/// Number of seconds to wait between sending buffer lag metrics to statsd.
const BUFFER_LAG_REPORT_SECONDS: u64 = 60;

/// Errors returned by [`start_ingest`] function.
#[derive(thiserror::Error, Debug)]
pub enum IngestError {
    /// Error from [`rdkafka`]
    #[error("Kafka error: {source}")]
    Kafka {
        /// Wrapped [`KafkaError`]
        #[from]
        source: KafkaError,
    },

    /// Error from [`deltalake::DeltaTable`]
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// Wrapped [`deltalake::DeltaTableError`]
        #[from]
        source: DeltaTableError,
    },

    /// Error from [`writer::DataWriter`]
    #[error("Writer error: {source}")]
    Writer {
        /// Wrapped [`DataWriterError`]
        #[from]
        source: DataWriterError,
    },

    /// Error occurred when writing a delta log checkpoint.
    #[error("CheckpointErrorError error: {source}")]
    CheckpointErrorError {
        /// The wrapped [`CheckpointError`]
        #[from]
        source: CheckpointError,
    },

    /// Error from [`WriteOffsetsError`]
    #[error("WriteOffsets error: {source}")]
    WriteOffsets {
        /// Wrapped [`WriteOffsetsError`]
        #[from]
        source: WriteOffsetsError,
    },

    /// Error from [`DeadLetterQueue`]
    #[error("DeadLetterQueue error: {source}")]
    DeadLetterQueueError {
        /// Wrapped [`DeadLetterQueueError`]
        #[from]
        source: DeadLetterQueueError,
    },

    /// Error from [`transforms`]
    #[error("TransformError: {source}")]
    Transform {
        /// Wrapped [`TransformError`]
        #[from]
        source: TransformError,
    },

    /// Error from [`serde_json`]
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        /// Wrapped [`serde_json::Error`]
        #[from]
        source: serde_json::Error,
    },

    /// Error from [`std::io`]
    #[error("IO Error: {source}")]
    IoError {
        /// Wrapped [`std::io::Error`]
        #[from]
        source: std::io::Error,
    },

    /// Error returned when [`IngestMetrics`] fails.
    #[error("IngestMetrics failed {source}")]
    IngestMetrics {
        /// Wrapped [`IngestMetricsError`]
        #[from]
        source: IngestMetricsError,
    },

    /// Error returned when a delta write fails.
    /// Ending Kafka offsets and counts for each partition are included to help identify the Kafka buffer that caused the write to fail.
    #[error(
        "Delta write failed: ending_offsets: {ending_offsets}, partition_counts: {partition_counts}, source: {source}"
    )]
    DeltaWriteFailed {
        /// Ending offsets for each partition that failed to be written to delta.
        ending_offsets: String,
        /// Message counts for each partition that failed to be written to delta.
        partition_counts: String,
        /// The underlying DataWriterError.
        source: DataWriterError,
    },

    /// Error returned when a message is received from Kafka that has already been processed.
    #[error(
        "Partition offset has already been processed - partition: {partition}, offset: {offset}"
    )]
    AlreadyProcessedPartitionOffset {
        /// The Kafka partition the message was received from
        partition: DataTypePartition,
        /// The Kafka offset of the message
        offset: DataTypeOffset,
    },

    /// Error returned when delta table is in an inconsistent state with the partition offsets being written.
    #[error("Delta table is in an inconsistent state: {0}")]
    InconsistentState(String),

    /// Error returned when a rebalance signal interrupts the run loop. This is handled by the runloop by resetting state, seeking the consumer and skipping the message.
    #[error("A rebalance signal exists while processing message")]
    RebalanceInterrupt,

    /// Error returned when the the offsets in delta log txn actions for assigned partitions have changed.
    #[error("Delta transaction log contains conflicting offsets for assigned partitions.")]
    ConflictingOffsets,

    /// Error returned when the delta schema has changed since the version used to write messages to the parquet buffer.
    #[error("Delta schema has changed and must be updated.")]
    DeltaSchemaChanged,

    /// Error returned if the committed Delta table version does not match the version specified by the commit attempt.
    #[error("Committed delta version {actual_version} does not match the version specified in the commit attempt {expected_version}")]
    UnexpectedVersionMismatch {
        /// The version specified in the commit attempt
        expected_version: DeltaDataTypeVersion,
        /// The version returned after the commit
        actual_version: DeltaDataTypeVersion,
    },
}

/// The enum to represent 'auto.offset.reset' options.
pub enum AutoOffsetReset {
    /// The "earliest" option. Messages will be ingested from the beginning of a partition on reset.
    Earliest,
    /// The "latest" option. Messages will be ingested from the end of a partition on reset.
    Latest,
}

impl AutoOffsetReset {
    /// The librdkafka config key used to specify an `auto.offset.reset` policy.
    pub const CONFIG_KEY: &'static str = "auto.offset.reset";
}

/// Options for configuring the behavior of the run loop executed by the [`start_ingest`] function.
pub struct IngestOptions {
    /// The Kafka broker string to connect to.
    pub kafka_brokers: String,
    /// The Kafka consumer group id to set to allow for multiple consumers per topic.
    pub consumer_group_id: String,
    /// Unique per topic per environment. **Must** be the same for all processes that are part of a single job.
    /// It's used as a prefix for the `txn` actions to track messages offsets between partition/writers.
    pub app_id: String,
    /// Offsets to seek to before the ingestion. Creates new delta log version with `txn` actions
    /// to store the offsets for each partition in delta table.
    /// Note that `seek_offsets` is not the starting offsets, as such, then first ingested message
    /// will be `seek_offset + 1` or the next successive message in a partition.
    /// This configuration is only applied when offsets are not already stored in delta table.
    /// Note that if offsets are already exists in delta table but they're lower than provided
    /// then the error will be returned as this could break the data integrity. If one would want to skip
    /// the data and write from the later offsets then supplying new `app_id` is a safer approach.
    pub seek_offsets: Option<Vec<(DataTypePartition, DataTypeOffset)>>,
    /// The policy to start reading from if both `txn` and `seek_offsets` has no specified offset
    /// for the partition. Either "earliest" or "latest". The configuration is also applied to the
    /// librdkafka `auto.offset.reset` config.
    pub auto_offset_reset: AutoOffsetReset,
    /// Max desired latency from when a message is received to when it is written and
    /// committed to the target delta table (in seconds)
    pub allowed_latency: u64,
    /// Number of messages to buffer before writing a record batch.
    pub max_messages_per_batch: usize,
    /// Desired minimum number of compressed parquet bytes to buffer in memory
    /// before writing to storage and committing a transaction.
    pub min_bytes_per_file: usize,
    /// A list of transforms to apply to the message before writing to delta lake.
    pub transforms: HashMap<String, String>,
    /// An optional dead letter table to write messages that fail deserialization, transformation or schema validation.
    pub dlq_table_uri: Option<String>,
    /// Transforms to apply to dead letters when writing to a delta table.
    pub dlq_transforms: HashMap<String, String>,
    /// If `true` then application will write checkpoints on each 10th commit.
    pub write_checkpoints: bool,
    /// Additional properties to initialize the Kafka consumer with.
    pub additional_kafka_settings: Option<HashMap<String, String>>,
    /// A statsd endpoint to send statistics to.
    pub statsd_endpoint: String,
}

impl Default for IngestOptions {
    fn default() -> Self {
        IngestOptions {
            kafka_brokers: "localhost:9092".to_string(),
            consumer_group_id: "kafka_delta_ingest".to_string(),
            app_id: "kafka_delta_ingest".to_string(),
            seek_offsets: None,
            auto_offset_reset: AutoOffsetReset::Earliest,
            allowed_latency: 300,
            max_messages_per_batch: 5000,
            min_bytes_per_file: 134217728,
            transforms: HashMap::new(),
            dlq_table_uri: None,
            dlq_transforms: HashMap::new(),
            additional_kafka_settings: None,
            write_checkpoints: false,
            statsd_endpoint: "localhost:8125".to_string(),
        }
    }
}
/// Executes a run loop to consume from a Kafka topic and write to a Delta table.
pub async fn start_ingest(
    topic: String,
    table_uri: String,
    opts: IngestOptions,
    cancellation_token: Arc<CancellationToken>,
) -> Result<(), IngestError> {
    info!(
        "Ingesting messages from {} Kafka topic to {} Delta table",
        topic, table_uri
    );
    info!("Using options: [allowed_latency={},max_messages_per_batch={},min_bytes_per_file={},write_checkpoints={}]",
        opts.allowed_latency,
        opts.max_messages_per_batch,
        opts.min_bytes_per_file,
        opts.write_checkpoints);

    // Initialize a RebalanceSignal to share between threads so it can be set when rebalance events are sent from Kafka and checked or cleared in the run loop.
    // We use an RwLock so we can quickly skip past the typical case in the run loop where the rebalance signal is a None without starving the writer.
    // See also `handle_rebalance`.
    let rebalance_signal = Arc::new(RwLock::new(None));

    // Configure and create the Kafka consumer
    // The KafkaContext holds an Arc RwLock of the rebalance signal so it can take out a write lock and set the signal when it receives a rebalance event.
    let kafka_consumer_context = KafkaContext {
        rebalance_signal: rebalance_signal.clone(),
    };
    let kafka_client_config = kafka_client_config_from_options(&opts);
    let consumer: StreamConsumer<KafkaContext> =
        kafka_client_config.create_with_context(kafka_consumer_context)?;
    let consumer = Arc::new(consumer);
    consumer.subscribe(&[topic.as_str()])?;

    // Initialize metrics
    let ingest_metrics = IngestMetrics::new(opts.statsd_endpoint.as_str())?;
    // Initialize partition assignment tracking
    let mut partition_assignment = PartitionAssignment::default();
    // Initialize the processor
    let mut ingest_processor = IngestProcessor::new(
        topic.clone(),
        table_uri.as_str(),
        consumer.clone(),
        opts,
        ingest_metrics.clone(),
    )
    .await?;

    // Write seek_offsets if it's supplied and has not been written yet
    ingest_processor.write_offsets_to_delta_if_any().await?;

    // Initialize a timer for reporting buffer lag periodically
    let mut last_buffer_lag_report: Option<Instant> = None;

    // The run loop
    while let Some(message) = consumer.stream().next().await {
        // Check for rebalance signal - skip the message if there is one.
        // After seek - we will re-consume the message and see it again.
        // See also `handle_rebalance` function.
        if let Err(e) = handle_rebalance(
            rebalance_signal.clone(),
            &mut partition_assignment,
            &mut ingest_processor,
        )
        .await
        {
            match e {
                IngestError::RebalanceInterrupt => continue,
                _ => {
                    return Err(e);
                }
            }
        }

        // Process the message if there wasn't a rebalance signal
        let message = message?;
        if let Err(e) = ingest_processor.process_message(message).await {
            match e {
                IngestError::AlreadyProcessedPartitionOffset { partition, offset } => {
                    debug!("Skipping message with partition {}, offset {} on topic {} because it was already processed", partition, offset, topic);
                    continue;
                }
                _ => return Err(e),
            }
        }

        // Record buffer lag periodically
        if should_record_buffer_lag(&last_buffer_lag_report) {
            record_buffer_lag(
                topic.as_str(),
                consumer.clone(),
                &partition_assignment,
                &ingest_metrics,
            )?;
            last_buffer_lag_report = Some(Instant::now());
        }

        // Complete the record batch if we should
        if ingest_processor.should_complete_record_batch() {
            ingest_metrics.batch_started();
            let timer = Instant::now();
            ingest_processor
                .complete_record_batch(&mut partition_assignment)
                .await?;
            ingest_metrics.batch_completed(ingest_processor.buffered_record_batch_count(), &timer);
        }

        // Complete the file if we should.
        // We may see conflicting offsets in the Delta log if a rebalance has happened prior to the write.
        // We may also observe a schema change created by an external writer.
        // In either case - the corrective action is to reset state and skip the current message.
        if ingest_processor.should_complete_file() {
            ingest_metrics.delta_write_started();
            let timer = Instant::now();
            match ingest_processor.complete_file(&partition_assignment).await {
                Err(IngestError::ConflictingOffsets) | Err(IngestError::DeltaSchemaChanged) => {
                    ingest_processor.reset_state(&mut partition_assignment)?;
                    continue;
                }
                Err(e) => {
                    ingest_metrics.delta_write_failed();
                    return Err(e);
                }
                Ok(v) => {
                    info!(
                        "Delta version {} completed in {} milliseconds.",
                        v,
                        timer.elapsed().as_millis()
                    );
                }
            }
            ingest_metrics.delta_write_completed(&timer);
        }

        // Exit if the cancellation token is set.
        if cancellation_token.is_cancelled() {
            return Ok(());
        }
    }

    Ok(())
}

/// Handles a [`RebalanceSignal`] if one exists.
/// The [`RebalanceSignal`] is wrapped in a tokio [`RwLock`] so that it can be written to from the thread that receives rebalance events from [`rdkafka`].
/// Writing a new rebalance signal is implemented in [`KafkaContext`].
/// When handling a signal, we take out a read lock first to avoid starving the write lock.
/// If a signal exists and indicates a new partition assignment, we take out a write lock so we can clear it after resetting state.
async fn handle_rebalance(
    rebalance_signal: Arc<RwLock<Option<RebalanceSignal>>>,
    partition_assignment: &mut PartitionAssignment,
    processor: &mut IngestProcessor,
) -> Result<(), IngestError> {
    // step 1 - use a read lock so we don't starve the write lock from `KafkaContext` to check if a rebalance signal exists.
    // if there is a rebalance assign signal - in step 2, we grab a write lock so we can reset state and clear signal.
    let rebalance_action = {
        let rebalance_signal = rebalance_signal.read().await;

        if let Some(rb) = rebalance_signal.as_ref() {
            match rb {
                RebalanceSignal::RebalanceAssign(_) => {
                    Some(RebalanceAction::ClearStateAndSkipMessage)
                }
                _ => Some(RebalanceAction::SkipMessage),
            }
        } else {
            None
        }
    };

    // step 2 - if there is a rebalance assign signal - we need to acquire the write lock so we can clear it after resetting state.
    // if there is a revoke signal - we should skip the message, but not bother altering state yet.
    match rebalance_action {
        Some(RebalanceAction::ClearStateAndSkipMessage) => {
            let mut rebalance_signal = rebalance_signal.write().await;
            match rebalance_signal.as_mut() {
                Some(RebalanceSignal::RebalanceAssign(partitions)) => {
                    info!(
                        "Handling rebalance assign. New assigned partitions are {:?}",
                        partitions
                    );
                    processor.table.update().await?;
                    partition_assignment.reset_with(partitions.as_slice());
                    processor.reset_state(partition_assignment)?;
                    *rebalance_signal = None;
                    Err(IngestError::RebalanceInterrupt)
                }
                _ => unreachable!(),
            }
        }
        Some(RebalanceAction::SkipMessage) => {
            debug!("Skipping message while awaiting rebalance");
            Err(IngestError::RebalanceInterrupt)
        }
        None => Ok(()),
    }
}

/// Returns a boolean indicating whether buffer lag should be reported based on the time of the last buffer lag report.
fn should_record_buffer_lag(last_buffer_lag_report: &Option<Instant>) -> bool {
    match last_buffer_lag_report {
        None => true,
        Some(last_buffer_lag_report)
            if last_buffer_lag_report.elapsed().as_secs() >= BUFFER_LAG_REPORT_SECONDS =>
        {
            true
        }
        _ => false,
    }
}

/// Sends buffer lag to statsd.
fn record_buffer_lag(
    topic: &str,
    consumer: Arc<StreamConsumer<KafkaContext>>,
    partition_assignment: &PartitionAssignment,
    ingest_metrics: &IngestMetrics,
) -> Result<(), KafkaError> {
    let partition_offsets = partition_assignment.nonempty_partition_offsets();
    let buffer_lags = calculate_lag(topic, consumer, &partition_offsets)?;

    ingest_metrics.buffer_lag(buffer_lags);

    Ok(())
}

/// Sends delta write lag to statsd.
fn record_write_lag(
    topic: &str,
    consumer: Arc<StreamConsumer<KafkaContext>>,
    partition_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    ingest_metrics: &IngestMetrics,
) -> Result<(), KafkaError> {
    let write_lags = calculate_lag(topic, consumer, partition_offsets)?;
    ingest_metrics.delta_lag(write_lags);
    Ok(())
}

/// Calculates lag for all partitions in the given list of partition offsets.
fn calculate_lag(
    topic: &str,
    consumer: Arc<StreamConsumer<KafkaContext>>,
    partition_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
) -> Result<Vec<DataTypeOffset>, KafkaError> {
    let high_watermarks = get_high_watermarks(topic, consumer, partition_offsets.keys().copied())?;
    let lags = partition_offsets
        .iter()
        .zip(high_watermarks.iter())
        .map(|((_, offset), high_watermark_offset)| high_watermark_offset - offset)
        .collect();

    Ok(lags)
}

/// Error returned when message deserialization fails.
/// This is handled by the run loop, and the message is treated as a dead letter.
#[derive(thiserror::Error, Debug)]
enum MessageDeserializationError {
    #[error("Kafka message contained empty payload")]
    EmptyPayload,
    #[error("Kafka message deserialization failed")]
    JsonDeserialization { dead_letter: DeadLetter },
}

/// Indicates whether a rebalance signal should simply skip the currently consumed message, or clear state and skip.
enum RebalanceAction {
    SkipMessage,
    ClearStateAndSkipMessage,
}

/// Holds state and encapsulates functionality required to process messages and write to delta.
struct IngestProcessor {
    topic: String,
    consumer: Arc<StreamConsumer<KafkaContext>>,
    transformer: Transformer,
    coercion_tree: CoercionTree,
    table: DeltaTable,
    delta_writer: DataWriter,
    value_buffers: ValueBuffers,
    delta_partition_offsets: HashMap<DataTypePartition, Option<DataTypeOffset>>,
    latency_timer: Instant,
    dlq: Box<dyn DeadLetterQueue>,
    opts: IngestOptions,
    ingest_metrics: IngestMetrics,
}

impl IngestProcessor {
    /// Creates a new ingest [`IngestProcessor`].
    async fn new(
        topic: String,
        table_uri: &str,
        consumer: Arc<StreamConsumer<KafkaContext>>,
        opts: IngestOptions,
        ingest_metrics: IngestMetrics,
    ) -> Result<IngestProcessor, IngestError> {
        let dlq = dead_letter_queue_from_options(&opts).await?;
        let transformer = Transformer::from_transforms(&opts.transforms)?;
        let table = delta_helpers::load_table(table_uri, HashMap::new()).await?;
        let coercion_tree = coercions::create_coercion_tree(&table.get_metadata()?.schema);
        let delta_writer = DataWriter::for_table(&table, HashMap::new())?;

        Ok(IngestProcessor {
            topic,
            consumer,
            transformer,
            coercion_tree,
            table,
            delta_writer,
            value_buffers: ValueBuffers::default(),
            latency_timer: Instant::now(),
            delta_partition_offsets: HashMap::new(),
            dlq,
            opts,
            ingest_metrics,
        })
    }

    /// If `opts.seek_offsets` is set then it calls the `offsets::write_offsets_to_delta` function.
    async fn write_offsets_to_delta_if_any(&mut self) -> Result<(), IngestError> {
        if let Some(ref offsets) = self.opts.seek_offsets {
            offsets::write_offsets_to_delta(&mut self.table, &self.opts.app_id, offsets).await?;
        }
        Ok(())
    }

    /// Processes a single message received from Kafka.
    /// This method deserializes, transforms and writes the message to buffers.
    async fn process_message<M>(&mut self, message: M) -> Result<(), IngestError>
    where
        M: Message + Send + Sync,
    {
        let partition = message.partition();
        let offset = message.offset();

        if !self.should_process_offset(partition, offset) {
            return Err(IngestError::AlreadyProcessedPartitionOffset { partition, offset });
        }

        // Deserialize
        match self.deserialize_message(&message) {
            Ok(mut value) => {
                self.ingest_metrics.message_deserialized();
                // Transform
                match self.transformer.transform(&mut value, Some(&message)) {
                    Ok(()) => {
                        self.ingest_metrics.message_transformed();
                        // Coerce data types
                        coercions::coerce(&mut value, &self.coercion_tree);
                        // Buffer
                        self.value_buffers.add(partition, offset, value)?;
                    }
                    Err(e) => {
                        warn!(
                            "Transform failed - partition {}, offset {}",
                            partition, offset
                        );
                        self.ingest_metrics.message_transform_failed();
                        self.dlq
                            .write_dead_letter(DeadLetter::from_failed_transform(&value, e))
                            .await?;
                    }
                }
            }
            Err(MessageDeserializationError::EmptyPayload) => {
                warn!(
                    "Empty payload for message - partition {}, offset {}",
                    partition, offset
                );
            }
            Err(MessageDeserializationError::JsonDeserialization { dead_letter }) => {
                warn!(
                    "Deserialization failed - partition {}, offset {}",
                    partition, offset
                );
                self.ingest_metrics.message_deserialization_failed();
                self.dlq.write_dead_letter(dead_letter).await?;
            }
        }

        Ok(())
    }

    /// Deserializes a message received from Kafka
    fn deserialize_message<M>(&mut self, msg: &M) -> Result<Value, MessageDeserializationError>
    where
        M: Message + Send + Sync,
    {
        // Deserialize the rdkafka message into a serde_json::Value
        let message_bytes = match msg.payload() {
            Some(bytes) => bytes,
            None => {
                return Err(MessageDeserializationError::EmptyPayload);
            }
        };

        self.ingest_metrics
            .message_deserialized_size(message_bytes.len());

        let value: Value = match serde_json::from_slice(message_bytes) {
            Ok(v) => v,
            Err(e) => {
                return Err(MessageDeserializationError::JsonDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(message_bytes, e),
                });
            }
        };

        Ok(value)
    }

    /// Writes the transformed messages currently held in buffer to parquet byte buffers.
    async fn complete_record_batch(
        &mut self,
        partition_assignment: &mut PartitionAssignment,
    ) -> Result<(), IngestError> {
        let ConsumedBuffers {
            values,
            partition_offsets,
            partition_counts,
        } = self.value_buffers.consume();
        partition_assignment.update_offsets(&partition_offsets);

        if values.is_empty() {
            return Ok(());
        }

        match self.delta_writer.write(values).await {
            Err(DataWriterError::PartialParquetWrite {
                skipped_values,
                sample_error,
            }) => {
                warn!(
                    "Partial parquet write, skipped {} values, sample ParquetError {:?}",
                    skipped_values.len(),
                    sample_error
                );

                let dead_letters = DeadLetter::vec_from_failed_parquet_rows(skipped_values);
                self.dlq.write_dead_letters(dead_letters).await?;
            }
            Err(e) => {
                return Err(IngestError::DeltaWriteFailed {
                    ending_offsets: serde_json::to_string(&partition_offsets).unwrap(),
                    partition_counts: serde_json::to_string(&partition_counts).unwrap(),
                    source: e,
                });
            }
            _ => { /* ok - noop */ }
        };

        Ok(())
    }

    /// Writes parquet buffers to a file in the destination delta table.
    async fn complete_file(
        &mut self,
        partition_assignment: &PartitionAssignment,
    ) -> Result<i64, IngestError> {
        // Reset the latency timer to track allowed latency for the next file
        self.latency_timer = Instant::now();
        let partition_offsets = partition_assignment.nonempty_partition_offsets();
        // Upload pending parquet file to delta store
        // TODO: remove it if we got conflict error? or it'll be considered as tombstone
        let add = self
            .delta_writer
            .write_parquet_files(&self.table.table_uri)
            .await?;
        // Record file sizes
        for a in add.iter() {
            self.ingest_metrics.delta_file_size(a.size);
        }
        // Try to commit
        let mut attempt_number: u32 = 0;
        let prepared_commit = {
            let mut tx = self.table.create_transaction(None);
            tx.add_actions(build_actions(
                &partition_offsets,
                self.opts.app_id.as_str(),
                add,
            ));
            tx.prepare_commit(None).await?
        };

        loop {
            self.table.update().await?;
            if !self.are_partition_offsets_match() {
                return Err(IngestError::ConflictingOffsets);
            }
            if self
                .delta_writer
                .update_schema(self.table.get_metadata()?)?
            {
                info!("Table schema has been updated");
                // Update the coercion tree to reflect the new schema
                let coercion_tree =
                    coercions::create_coercion_tree(&self.table.get_metadata()?.schema);
                let _ = std::mem::replace(&mut self.coercion_tree, coercion_tree);

                return Err(IngestError::DeltaSchemaChanged);
            }
            let version = self.table.version + 1;
            let commit_result = self
                .table
                .try_commit_transaction(&prepared_commit, version)
                .await;
            match commit_result {
                Ok(v) => {
                    if v != version {
                        return Err(IngestError::UnexpectedVersionMismatch {
                            expected_version: version,
                            actual_version: v,
                        });
                    }
                    assert_eq!(v, version);
                    for (p, o) in &partition_offsets {
                        self.delta_partition_offsets.insert(*p, Some(*o));
                    }
                    if self.opts.write_checkpoints {
                        try_create_checkpoint(&mut self.table, version).await?;
                    }
                    record_write_lag(
                        self.topic.as_str(),
                        self.consumer.clone(),
                        &partition_offsets,
                        &self.ingest_metrics,
                    )?;
                    return Ok(version);
                }
                Err(e) => match e {
                    DeltaTableError::VersionAlreadyExists(_)
                        if attempt_number > DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS + 1 =>
                    {
                        error!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing", DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS);
                        return Err(e.into());
                    }
                    DeltaTableError::VersionAlreadyExists(_) => {
                        attempt_number += 1;
                        warn!("Transaction attempt failed. Incrementing attempt number to {} and retrying", attempt_number);
                    }
                    DeltaTableError::StorageError {
                        source:
                            StorageError::DynamoDb {
                                source: DynamoError::NonAcquirableLock,
                            },
                    } => {
                        error!("Delta write failed.. DeltaTableError: {}", e);
                        return Err(IngestError::InconsistentState(
                            "The remote dynamodb lock is non-acquirable!".to_string(),
                        ));
                    }
                    _ => {
                        return Err(e.into());
                    }
                },
            }
        }
    }

    /// Resets all current state to the correct starting points represented by the current partition assignment.
    fn reset_state(
        &mut self,
        partition_assignment: &mut PartitionAssignment,
    ) -> Result<(), IngestError> {
        // Reset all stored state
        self.delta_writer.reset();
        self.value_buffers.reset();
        self.delta_partition_offsets.clear();
        let partitions: Vec<DataTypePartition> = partition_assignment.assigned_partitions();
        // Update offsets stored in PartitionAssignment to the latest from the delta log
        for partition in partitions.iter() {
            let txn_app_id = txn_app_id_for_partition(self.opts.app_id.as_str(), *partition);
            let version = last_txn_version(&self.table, &txn_app_id);
            partition_assignment.assignment.insert(*partition, version);
            self.delta_partition_offsets.insert(*partition, version);
        }
        // Seek the consumer to the correct offset for each partition
        self.seek_consumer(partition_assignment)?;
        Ok(())
    }

    /// Seeks the Kafka consumer to the appropriate offsets based on the [`PartitionAssignment`].
    fn seek_consumer(&self, partition_assignment: &PartitionAssignment) -> Result<(), IngestError> {
        let mut log_message = String::new();

        for (p, offset) in partition_assignment.assignment.iter() {
            match offset {
                Some(o) if *o == 0 => {
                    // MARK: workaround for rdkafka error when attempting seek to offset 0
                    info!("Seeking consumer to beginning for partition {}. Delta log offset is 0, but seek to zero is not possible.", p);
                    self.consumer
                        .seek(&self.topic, *p, Offset::Beginning, Timeout::Never)?;
                }
                Some(o) => {
                    self.consumer
                        .seek(&self.topic, *p, Offset::Offset(*o), Timeout::Never)?;

                    log_message = log_message.add(format!("{}:{},", p, o).as_str());
                }
                None => match self.opts.auto_offset_reset {
                    AutoOffsetReset::Earliest => {
                        info!("Seeking consumer to beginning for partition {}. Partition has no stored offset but 'auto.offset.reset' is earliest", p);
                        self.consumer
                            .seek(&self.topic, *p, Offset::Beginning, Timeout::Never)?;
                    }
                    AutoOffsetReset::Latest => {
                        info!("Seeking consumer to end for partition {}. Partition has no stored offset but 'auto.offset.reset' is latest", p);
                        self.consumer
                            .seek(&self.topic, *p, Offset::End, Timeout::Never)?;
                    }
                },
            };
        }
        if !log_message.is_empty() {
            info!("Seeking consumer to partition offsets: [{}]", log_message);
        }
        Ok(())
    }

    /// Returns a boolean indicating whether a message with `partition` and `offset` should be processed given current state.
    fn should_process_offset(&self, partition: DataTypePartition, offset: DataTypeOffset) -> bool {
        if let Some(Some(written_offset)) = self.delta_partition_offsets.get(&partition) {
            if offset <= *written_offset {
                debug!(
                    "Message with partition {} offset {} on topic {} is already in delta log so skipping.",
                    partition, offset, self.topic
                );
                return false;
            }
        }

        true
    }

    /// Returns a boolean indicating whether a record batch should be written based on current state.
    fn should_complete_record_batch(&self) -> bool {
        let elapsed_millis = self.latency_timer.elapsed().as_millis();

        let should = self.value_buffers.len() == self.opts.max_messages_per_batch
            || elapsed_millis >= (self.opts.allowed_latency * 1000) as u128;

        debug!(
            "Should complete record batch - latency test: {} >= {}",
            elapsed_millis,
            (self.opts.allowed_latency * 1000) as u128
        );
        debug!(
            "Should complete record batch - buffer length test: {} >= {}",
            self.value_buffers.len(),
            self.opts.max_messages_per_batch
        );

        should
    }

    /// Returns a boolean indicating whether a delta file should be completed based on current state.
    fn should_complete_file(&self) -> bool {
        let elapsed_secs = self.latency_timer.elapsed().as_secs();

        let should = self.delta_writer.buffer_len() >= self.opts.min_bytes_per_file
            || elapsed_secs >= self.opts.allowed_latency;

        debug!(
            "Should complete file - latency test: {} >= {}",
            elapsed_secs, self.opts.allowed_latency
        );
        debug!(
            "Should complete file - num bytes test: {} >= {}",
            self.delta_writer.buffer_len(),
            self.opts.min_bytes_per_file
        );

        should
    }

    /// Returns a boolean indicating whether the partition offsets currently held in memory match those stored in the delta log.
    fn are_partition_offsets_match(&self) -> bool {
        let mut result = true;
        for (partition, offset) in &self.delta_partition_offsets {
            let version = last_txn_version(
                &self.table,
                &txn_app_id_for_partition(self.opts.app_id.as_str(), *partition),
            );

            if let Some(version) = version {
                match offset {
                    Some(offset) if *offset == version => (),
                    _ => {
                        info!(
                            "Conflicting offset for partition {}: offset={:?}, delta={}",
                            partition, offset, version
                        );
                        result = false;
                    }
                }
            }
        }
        result
    }

    fn buffered_record_batch_count(&self) -> usize {
        self.delta_writer.buffered_record_batch_count()
    }
}

/// Enum that represents a signal of an asynchronously received rebalance event that must be handled in the run loop.
/// Used to preserve correctness of messages stored in buffer after handling a rebalance event.
#[derive(Debug, PartialEq, Clone)]
enum RebalanceSignal {
    RebalanceRevoke,
    RebalanceAssign(Vec<DataTypePartition>),
}

/// Contains the partition to offset map for all partitions assigned to the consumer.
#[derive(Default)]
struct PartitionAssignment {
    assignment: HashMap<DataTypePartition, Option<DataTypeOffset>>,
}

impl PartitionAssignment {
    /// Resets the [`PartitionAssignment`] with a new list of partitions.
    /// Offsets are set as [`None`] for all partitions.
    fn reset_with(&mut self, partitions: &[DataTypePartition]) {
        self.assignment.clear();
        for p in partitions {
            self.assignment.insert(*p, None);
        }
    }

    /// Updates the offsets for each partition stored in the [`PartitionAssignment`].
    fn update_offsets(&mut self, updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>) {
        for (k, v) in updated_offsets {
            if let Some(entry) = self.assignment.get_mut(k) {
                *entry = Some(*v);
            }
        }
    }

    /// Returns the full list of assigned partitions as a [`Vec`] whether offsets are recorded for them in-memory or not.
    fn assigned_partitions(&self) -> Vec<DataTypePartition> {
        self.assignment.keys().copied().collect()
    }

    /// Returns a copy of the current partition offsets as a [`HashMap`] for all partitions that have an offset stored in memory.
    /// Partitions that do not have an offset stored in memory (offset is [`None`]) are **not** included in the returned HashMap.
    fn nonempty_partition_offsets(&self) -> HashMap<DataTypePartition, DataTypeOffset> {
        let partition_offsets = self
            .assignment
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|o| (*k, *o)))
            .collect();

        partition_offsets
    }
}

/// Implements rdkafka [`ClientContext`] to handle rebalance events sent to the rdkafka [`Consumer`].
struct KafkaContext {
    rebalance_signal: Arc<RwLock<Option<RebalanceSignal>>>,
}

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        let rebalance_signal = self.rebalance_signal.clone();
        match rebalance {
            Rebalance::Revoke(_) => {
                info!("PRE_REBALANCE - Revoke");
                let _ = tokio::spawn(async move {
                    rebalance_signal
                        .write()
                        .await
                        .replace(RebalanceSignal::RebalanceRevoke);
                });
            }
            Rebalance::Assign(tpl) => {
                debug!("PRE_REBALANCE - Assign {:?}", tpl);
            }
            Rebalance::Error(e) => {
                panic!("PRE_REBALANCE - Unexpected Kafka error {:?}", e);
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        let rebalance_signal = self.rebalance_signal.clone();
        match rebalance {
            Rebalance::Revoke(_) => {
                debug!("POST_REBALANCE - Revoke");
            }
            Rebalance::Assign(tpl) => {
                let partitions = partition_vec_from_topic_partition_list(tpl);
                info!("POST_REBALANCE - Assign {:?}", partitions);

                let _ = tokio::spawn(async move {
                    rebalance_signal
                        .write()
                        .await
                        .replace(RebalanceSignal::RebalanceAssign(partitions));
                });
            }
            Rebalance::Error(e) => {
                panic!("POST_REBALANCE - Unexpected Kafka error {:?}", e);
            }
        }
    }
}

/// Creates an rdkafka [`ClientConfig`] from the provided [`IngestOptions`].
fn kafka_client_config_from_options(opts: &IngestOptions) -> ClientConfig {
    let mut kafka_client_config = ClientConfig::new();
    if let Ok(cert_pem) = std::env::var("KAFKA_DELTA_INGEST_CERT") {
        kafka_client_config.set("ssl.certificate.pem", cert_pem);
    }
    if let Ok(key_pem) = std::env::var("KAFKA_DELTA_INGEST_KEY") {
        kafka_client_config.set("ssl.key.pem", key_pem);
    }

    let auto_offset_reset = match opts.auto_offset_reset {
        AutoOffsetReset::Earliest => "earliest",
        AutoOffsetReset::Latest => "latest",
    };
    kafka_client_config.set(AutoOffsetReset::CONFIG_KEY, auto_offset_reset);

    if let Some(additional) = &opts.additional_kafka_settings {
        for (k, v) in additional.iter() {
            kafka_client_config.set(k, v);
        }
    }
    kafka_client_config
        .set("bootstrap.servers", opts.kafka_brokers.clone())
        .set("group.id", opts.consumer_group_id.clone())
        .set("enable.auto.commit", "false");

    kafka_client_config
}

/// Creates a [`DeadLetterQueue`] to send broken messages to based on options.
async fn dead_letter_queue_from_options(
    opts: &IngestOptions,
) -> Result<Box<dyn DeadLetterQueue>, DeadLetterQueueError> {
    dead_letters::dlq_from_opts(DeadLetterQueueOptions {
        delta_table_uri: opts.dlq_table_uri.clone(),
        dead_letter_transforms: opts.dlq_transforms.clone(),
        write_checkpoints: opts.write_checkpoints,
    })
    .await
}

/// Creates a vec of partition numbers from a topic partition list.
fn partition_vec_from_topic_partition_list(
    topic_partition_list: &TopicPartitionList,
) -> Vec<DataTypePartition> {
    topic_partition_list
        .to_topic_map()
        .iter()
        .map(|((_, p), _)| *p)
        .collect()
}

/// Fetches high watermarks (latest offsets) from Kafka from the iterator of partitions.
fn get_high_watermarks<I>(
    topic: &str,
    consumer: Arc<StreamConsumer<KafkaContext>>,
    partitions: I,
) -> Result<Vec<i64>, KafkaError>
where
    I: Iterator<Item = DataTypePartition>,
{
    partitions
        .map(|partition| {
            consumer
                .fetch_watermarks(topic, partition, Timeout::Never)
                .map(|(_, latest_offset)| latest_offset)
        })
        .collect()
}
