//! Implementations supporting the kafka-delta-ingest daemon

// #![deny(warnings)]
// #![deny(missing_docs)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate strum_macros;

#[cfg(test)]
extern crate serde_json;

use deltalake::{action, DeltaTableError};
use futures::stream::StreamExt;
use log::{debug, error, info, warn};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, ConsumerContext, MessageStream, Rebalance, StreamConsumer},
    error::KafkaError,
    message::{BorrowedMessage, OwnedMessage},
    util::{Timeout, TokioRuntime},
    ClientContext, Message, Offset, TopicPartitionList,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
// use tokio::sync::{mpsc::Sender, Mutex};
use std::sync::Mutex;
use tokio::{
    sync::mpsc::{self, error::SendError, Receiver, Sender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

mod dead_letters;
mod delta_helpers;
pub mod deltalake_ext;
mod instrumentation;
mod transforms;

#[allow(unused_imports)]
use crate::{
    dead_letters::*,
    deltalake_ext::{DeltaWriter, DeltaWriterError},
    instrumentation::{Instrumentation, StatTypes, Statistic},
    transforms::*,
};
use deltalake::action::{Action, Add};
use deltalake::storage::s3::dynamodb_lock::DynamoError;
use deltalake::storage::StorageError;

type DataTypePartition = i32;
type DataTypeOffset = i64;

const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS: u32 = 10_000_000;
const BUFFER_LAG_REPORT_SECONDS: u64 = 60;

const AUTO_OFFSET_RESET_CONFIG_KEY: &str = "auto.offset.reset";
const AUTO_OFFSET_RESET_CONFIG_VALUE_EARLIEST: &str = "earliest";
const AUTO_OFFSET_RESET_CONFIG_VALUE_LATEST: &str = "latest";

#[derive(thiserror::Error, Debug)]
pub enum IngestError {
    /// Error from [`rdkafka`]
    #[error("Kafka error: {source}")]
    Kafka {
        /// Wrapped [`KafkaError`]
        #[from]
        source: KafkaError,
    },

    #[error("Kafka source error: {source}")]
    KafkaSource {
        ///
        #[from]
        source: SendError<KafkaEvent>,
    },

    /// Error from [`deltalake::DeltaTable`]
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// Wrapped [`deltalake::DeltaTableError`]
        #[from]
        source: DeltaTableError,
    },

    /// Error from [`DeltaWriter`]
    #[error("DeltaWriter error: {source}")]
    DeltaWriter {
        /// Wrapped [`DeltaWriterError`]
        #[from]
        source: DeltaWriterError,
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
        /// The underlying DeltaWriterError.
        source: DeltaWriterError,
    },

    /// Error returned when delta table is in inconsistent state.
    #[error("Delta table is in inconsistent state: {0}")]
    InconsistentState(String),
}

/// Error returned when the string passed to [`StartingOffsets::from_string`] is invalid.
#[derive(thiserror::Error, Debug)]
#[error("Error returned when the value")]
pub struct StartingOffsetsParseError {
    /// The string that failed to parse.
    pub string_to_parse: String,
    /// serde_json error returned when trying to parse the string as explicit offsets.
    pub error_message: String,
}

/// HashMap containing a specific offset to start from for each partition.
pub type StartingPartitionOffsets = HashMap<DataTypePartition, DataTypeOffset>;

/// Enum representing available starting offset options.
/// When specifying explicit starting offsets, the JSON string passed must have a string key representing the partition
/// and a u64 value representing the offset to start from.
///
/// # Example:
///
/// ```
/// use maplit::hashmap;
/// use kafka_delta_ingest::StartingOffsets;
///
/// let starting_offsets = StartingOffsets::from_string(r#"{"0":21,"1":52,"2":3}"#.to_string());
///
/// match starting_offsets {
///     Ok(StartingOffsets::Explicit(starting_offsets)) => {
///         assert_eq!(hashmap!{0 => 21, 1 => 52, 2 => 3}, starting_offsets);
///     }
///     _ => assert!(false, "This won't happen if you're JSON is formatted correctly.")
/// }
/// ```
#[derive(Debug, serde::Deserialize, Clone, PartialEq)]
pub enum StartingOffsets {
    /// Start from earliest available Kafka offsets for partition offsets not stored in the delta log.
    Earliest,
    /// Start from latest available Kafka offsets for partition offsets not stored in the delta log.
    Latest,
    /// Start from explicit partition offsets when no offsets are stored in the delta log.
    Explicit(StartingPartitionOffsets),
}

impl StartingOffsets {
    /// Parses a string as [`StartingOffsets`].
    /// Returns [`StartingOffsetsParseError`] if the string is not parseable.
    pub fn from_string(s: String) -> Result<StartingOffsets, StartingOffsetsParseError> {
        match s.as_str() {
            "earliest" => Ok(StartingOffsets::Earliest),
            "latest" => Ok(StartingOffsets::Latest),
            maybe_json => {
                let starting_partition_offsets: StartingPartitionOffsets =
                    serde_json::from_str(maybe_json).map_err(|e| StartingOffsetsParseError {
                        string_to_parse: s.clone(),
                        error_message: e.to_string(),
                    })?;

                Ok(StartingOffsets::Explicit(starting_partition_offsets))
            }
        }
    }
}

pub struct IngestOptions {
    /// The Kafka broker string to connect to.
    pub kafka_brokers: String,
    /// The Kafka consumer group id to set to allow for multiple consumers per topic.
    pub consumer_group_id: String,
    /// Unique per topic per environment. Must be the same for all processes that are part of a single job.
    pub app_id: String,
    /// Offsets to start from. This may be `earliest` or `latest` to start from the earliest or latest available offsets in Kafka,
    /// or a JSON string specifying the explicit offset to start from for each partition.
    /// This configuration is only applied when offsets are not already stored in delta lake.
    /// When using explicit starting offsets, `auto.offset.reset` may also be specified in `additional_kafka_settings` to control
    /// reset behavior in case partitions are added to the topic in the future.
    pub starting_offsets: StartingOffsets,
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
            starting_offsets: StartingOffsets::Earliest,
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

/// ...
#[derive(Debug)]
pub enum KafkaEvent {
    Message(OwnedMessage),
    Rebalance(RebalanceSignal),
    RebalanceCompleteMarker,
}

enum MessageDeserializationError {
    EmptyPayload,
    JsonDeserialization { dead_letter: DeadLetter },
}

pub async fn start_ingest(
    topic: String,
    table_uri: String,
    opts: IngestOptions,
    cancellation_token: Arc<CancellationToken>,
) -> Result<(), IngestError> {
    // Start stats listener and get a sender
    // let stats_sender =
    // instrumentation::init_stats(opts.statsd_endpoint.as_str(), opts.app_id.as_str())?;

    // Initialize the channel for receiving rebalance and message events
    let (event_sender, event_receiver) = mpsc::channel::<KafkaEvent>(1024);

    // Configure and create the Kafka consumer
    let kafka_client_config = kafka_client_config_from_options(&opts);
    let kafka_consumer_context = Context::new(event_sender.clone());
    let consumer: StreamConsumer<Context> =
        kafka_client_config.create_with_context(kafka_consumer_context)?;
    let consumer = Arc::new(consumer);
    consumer.subscribe(&[topic.as_str()])?;

    // Start the message source
    let source_handle = start_source(
        consumer.clone(),
        event_sender.clone(),
        cancellation_token.clone(),
    );

    // Start processing events
    let processor = ProcessingState::new(topic, table_uri.as_str(), consumer.clone(), opts).await?;
    let processor_handle = start_processing(
        event_sender.clone(),
        event_receiver,
        processor,
        cancellation_token.clone(),
    );

    // TODO: handle JoinError
    tokio::select! {
        Ok(result) = source_handle => {
            info!("Source terminated.");
            result
        }
        Ok(result) = processor_handle => {
            info!("Processor terminated.");
            result
        }
    }
}

fn start_processing(
    mut event_sender: Sender<KafkaEvent>,
    mut event_receiver: Receiver<KafkaEvent>,
    mut processor: ProcessingState,
    cancellation_token: Arc<CancellationToken>,
) -> JoinHandle<Result<(), IngestError>> {
    tokio::spawn(async move {
        while let Some(event) = event_receiver.recv().await {
            match event {
                KafkaEvent::Message(m) => {
                    if !processor.rebalancing {
                        processor.process_message(m).await?;
                    } else {
                        warn!(
                            "Skipping message at partition {} offset {} received during rebalance",
                            m.partition(),
                            m.offset()
                        );
                    }
                }
                KafkaEvent::Rebalance(r) => {
                    processor.process_rebalance(r, &event_sender).await?;
                }
                KafkaEvent::RebalanceCompleteMarker => {
                    warn!("Received RebalanceCompleteMarker - messages received after the marker will be processed.");
                    processor.rebalancing = false;
                }
            }
            if cancellation_token.is_cancelled() {
                info!("Cancellation token is set. Stopping ingest process.");
                return Ok(());
            }
        }

        Ok(())
    })
}

async fn dead_letter_queue_from_options(
    opts: &IngestOptions,
) -> Result<Box<dyn DeadLetterQueue>, DeadLetterQueueError> {
    Ok(dead_letters::dlq_from_opts(DeadLetterQueueOptions {
        delta_table_uri: opts.dlq_table_uri.clone(),
        dead_letter_transforms: opts.dlq_transforms.clone(),
        write_checkpoints: opts.write_checkpoints,
    })
    .await?)
}

fn start_source(
    consumer: Arc<StreamConsumer<Context>>,
    event_sender: Sender<KafkaEvent>,
    cancellation_token: Arc<CancellationToken>,
) -> JoinHandle<Result<(), IngestError>> {
    tokio::spawn(async move {
        while let Some(message) = consumer.stream().next().await {
            event_sender
                .send(KafkaEvent::Message(message.unwrap().detach()))
                .await?;

            if cancellation_token.is_cancelled() {
                info!("Cancellation token is set. Stopping source.");
                return Ok(());
            }
        }

        Ok(())
    })
}

fn kafka_client_config_from_options(opts: &IngestOptions) -> ClientConfig {
    let mut kafka_client_config = ClientConfig::new();

    if let Ok(cert_pem) = std::env::var("KAFKA_DELTA_INGEST_CERT") {
        kafka_client_config.set("ssl.certificate.pem", cert_pem);
    }

    if let Ok(key_pem) = std::env::var("KAFKA_DELTA_INGEST_KEY") {
        kafka_client_config.set("ssl.key.pem", key_pem);
    }

    if opts.starting_offsets == StartingOffsets::Latest {
        kafka_client_config.set(
            AUTO_OFFSET_RESET_CONFIG_KEY,
            AUTO_OFFSET_RESET_CONFIG_VALUE_LATEST,
        );
    } else if opts.starting_offsets == StartingOffsets::Earliest {
        kafka_client_config.set(
            AUTO_OFFSET_RESET_CONFIG_KEY,
            AUTO_OFFSET_RESET_CONFIG_VALUE_EARLIEST,
        );
    }

    if let Some(additional) = &opts.additional_kafka_settings {
        for (k, v) in additional.iter() {
            info!("Applying additional kafka setting {} = {}", k, v);
            kafka_client_config.set(k, v);
        }
    }

    kafka_client_config
        .set("bootstrap.servers", opts.kafka_brokers.clone())
        .set("group.id", opts.consumer_group_id.clone())
        .set("enable.auto.commit", "false");

    kafka_client_config
}

struct ProcessingState {
    topic: String,
    consumer: Arc<StreamConsumer<Context>>,
    transformer: Transformer,
    delta_writer: DeltaWriter,
    partition_assignment: PartitionAssignment,
    value_buffers: ValueBuffers,
    delta_partition_offsets: HashMap<DataTypePartition, Option<DataTypeOffset>>,
    latency_timer: Instant,
    last_buffer_lag_report: Option<Instant>,
    dlq: Box<dyn DeadLetterQueue>,
    opts: IngestOptions,
    rebalancing: bool,
}

impl ProcessingState {
    async fn new(
        topic: String,
        table_uri: &str,
        consumer: Arc<StreamConsumer<Context>>,
        opts: IngestOptions,
    ) -> Result<ProcessingState, IngestError> {
        let dlq = dead_letter_queue_from_options(&opts).await?;
        let transformer = Transformer::from_transforms(&opts.transforms)?;
        Ok(ProcessingState {
            topic,
            consumer,
            transformer,
            delta_writer: DeltaWriter::for_table_uri(table_uri).await?,
            value_buffers: ValueBuffers::default(),
            partition_assignment: PartitionAssignment::default(),
            latency_timer: Instant::now(),
            delta_partition_offsets: HashMap::new(),
            last_buffer_lag_report: None,
            dlq,
            opts,
            rebalancing: false,
        })
    }

    fn should_process_offset(&self, partition: DataTypePartition, offset: DataTypeOffset) -> bool {
        if let Some(Some(written_offset)) = self.delta_partition_offsets.get(&partition) {
            if offset <= *written_offset {
                return false;
            }
        }

        if let Some(Some(buffered_offset)) = self
            .value_buffers
            .buffers
            .get(&partition)
            .map(|b| b.last_offset)
        {
            if offset <= buffered_offset {
                return false;
            }
        }

        if let StartingOffsets::Explicit(starting_offsets) = &self.opts.starting_offsets {
            if starting_offsets.get(&partition).map(|o| *o >= offset) == Some(true) {
                return false;
            }
        }

        return true;
    }

    async fn process_message<M>(&mut self, message: M) -> Result<(), IngestError>
    where
        M: Message + Send + Sync,
    {
        let log_suffix = format!(
            " - partition {}, offset {}, topic - {}",
            message.partition(),
            message.offset(),
            self.topic,
        );
        debug!("Message received{}", log_suffix);

        if !self.should_process_offset(message.partition(), message.offset()) {
            debug!("Skipping - should not process{}", log_suffix);

            return Ok(());
        }

        match self.deserialize_message(&message) {
            Ok(mut value) => match self.transformer.transform(&mut value, Some(&message)) {
                Ok(()) => {
                    self.value_buffers
                        .add(message.partition(), message.offset(), value);
                }
                Err(e) => {
                    warn!("Transform failed{}", log_suffix);
                    self.dlq
                        .write_dead_letter(DeadLetter::from_failed_transform(&value, e))
                        .await?;
                }
            },
            Err(MessageDeserializationError::EmptyPayload) => {
                warn!("Empty payload for message{}", log_suffix);
            }
            Err(MessageDeserializationError::JsonDeserialization { dead_letter }) => {
                warn!("Deserialization failed{}", log_suffix);
                self.dlq.write_dead_letter(dead_letter).await?;
            }
        }

        // TODO: The call stack for should_complete_record_batch, complete_record_batch, should_complete_file, complete_file
        // has a funny smell. The nesting of *complete_file calls bothers me for one thing.
        // I'd like to do a bit more restructuring and maybe flatten these out.
        if self.should_complete_record_batch() {
            info!("Completing record batch{}", log_suffix);
            self.complete_record_batch().await?;
        }

        Ok(())
    }

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

        // TODO: record stat for MessageSize

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

    async fn process_rebalance(
        &mut self,
        rebalance_signal: RebalanceSignal,
        event_sender: &Sender<KafkaEvent>,
    ) -> Result<(), IngestError> {
        info!("Processing rebalance signal - {:?}", rebalance_signal);
        match rebalance_signal {
            RebalanceSignal::PreRebalanceRevoke => {
                //
            }
            RebalanceSignal::PreRebalanceAssign(_partitions) => {
                //
            }
            RebalanceSignal::PostRebalanceRevoke => {
                //
            }
            RebalanceSignal::PostRebalanceAssign(partitions) => {
                self.rebalancing = true;
                let tpl = topic_partition_list_from_partitions(self.topic.as_str(), &partitions);
                info!("Pausing consumer");
                self.consumer.pause(&tpl)?;
                self.partition_assignment.reset_with(partitions.as_slice());
                self.reset_state().await?;
                info!("Sending rebalance complete marker");
                event_sender
                    .send(KafkaEvent::RebalanceCompleteMarker)
                    .await?;
                info!("Resuming consumer");
                self.consumer.resume(&tpl)?;
            }
        }
        Ok(())
    }

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

    async fn complete_record_batch(&mut self) -> Result<(), IngestError> {
        info!("Record batch started");

        let (values, partition_offsets, partition_counts) = self.consume_value_buffers().await?;

        if values.is_empty() {
            return Ok(());
        }

        // TODO: Use for recording record batch write duration
        let record_batch_timer = Instant::now();

        match self.delta_writer.write(values).await {
            Err(DeltaWriterError::PartialParquetWrite {
                skipped_values,
                sample_error,
            }) => {
                warn!(
                    "Partial parquet write, skipped {}, ParquetError {:?}",
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

        let elapsed_millis = record_batch_timer.elapsed().as_millis();
        info!("Record batch completed in {} millis", elapsed_millis);
        // TODO: record stats - RecordBatchCompleted, RecordBatchWriteDuration

        // Finalize file and write Delta transaction
        if self.should_complete_file() {
            self.complete_file(&partition_offsets).await?;
        }

        Ok(())
    }

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

    fn are_partition_offsets_match(&self) -> bool {
        let mut result = true;
        for (partition, offset) in &self.delta_partition_offsets {
            let version = self.delta_writer.last_transaction_version(
                &delta_helpers::txn_app_id_for_partition(self.opts.app_id.as_str(), *partition),
            );

            if let Some(version) = version {
                match offset {
                    Some(offset) if *offset == version => (),
                    _ => {
                        info!(
                            "Conflicting offset for partition {}: state={:?}, delta={:?}",
                            partition, offset, version
                        );
                        result = false;
                    }
                }
            }
        }
        result
    }

    async fn complete_file(
        &mut self,
        partition_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    ) -> Result<(), IngestError> {
        // Reset the latency timer to track allowed latency for the next file
        self.latency_timer = Instant::now();

        info!("Completing file");

        // TODO: use for recording DeltaWriteDuration
        let delta_write_timer = Instant::now();

        // upload pending parquet file to delta store
        // TODO remove it if we got conflict error? or it'll be considered as tombstone

        let add = self.delta_writer.write_parquet_files().await?;

        for a in add.iter() {
            // TODO: record DeltaAddFileSize for each file
        }

        let mut attempt_number: u32 = 0;

        let prepared_commit = {
            let mut tx = self.delta_writer.table.create_transaction(None);
            tx.add_actions(delta_helpers::build_actions(
                partition_offsets,
                self.opts.app_id.as_str(),
                add,
            ));
            tx.prepare_commit(None).await?
        };

        loop {
            self.delta_writer.update_table().await?;

            if !self.are_partition_offsets_match() {
                warn!("Transaction attempt failed. Delta log contains conflicting offsets. Resetting state.");
                self.reset_state().await?;

                // TODO: delete parquet file
                return Ok(());
            }

            if self.delta_writer.update_schema()? {
                warn!("Transaction attempt failed. Delta log contains conflicting offsets. Resetting state.");
                self.reset_state().await?;

                // TODO: delete parquet file
                return Ok(());
            }

            let version = self.delta_writer.table_version() + 1;
            let commit_result = self
                .delta_writer
                .table
                .try_commit_transaction(&prepared_commit, version)
                .await;

            match commit_result {
                Ok(v) => {
                    assert_eq!(v, version);

                    for (p, o) in partition_offsets {
                        self.delta_partition_offsets.insert(*p, Some(*o));
                    }

                    if self.opts.write_checkpoints {
                        self.delta_writer.try_create_checkpoint(version).await?;
                    }

                    let elapsed_millis = delta_write_timer.elapsed().as_millis() as i64;
                    info!(
                        "Delta write for version {} has completed in {} millis",
                        version, elapsed_millis
                    );

                    // TODO: record stat for DeltaWriteCompleted and DeltaWriteDuration

                    return Ok(());
                }
                Err(e) => match e {
                    DeltaTableError::VersionAlreadyExists(_)
                        if attempt_number > DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS + 1 =>
                    {
                        error!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing.", DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS);
                        // TODO: record stat for DeltaWriteFailed

                        return Err(e.into());
                    }
                    DeltaTableError::VersionAlreadyExists(_) => {
                        attempt_number += 1;
                        error!("Transaction attempt failed. Incrementing attempt number to {} and retrying.", attempt_number);
                    }
                    DeltaTableError::StorageError {
                        source:
                            StorageError::DynamoDb {
                                source: DynamoError::NonAcquirableLock,
                            },
                    } => {
                        error!("Delta write failed {}", e);
                        // TODO: record stat for DeltaWriteFailed
                        return Err(IngestError::InconsistentState(
                            "The remote dynamodb lock is non-acquirable!".to_string(),
                        ));
                    }
                    _ => {
                        // TODO: record stat for DeltaWriteFailed
                        return Err(e.into());
                    }
                },
            }
        }
    }

    async fn consume_value_buffers(
        &mut self,
    ) -> Result<
        (
            Vec<Value>,
            HashMap<DataTypePartition, DataTypeOffset>,
            HashMap<DataTypePartition, usize>,
        ),
        IngestError,
    > {
        let ConsumedBuffers {
            values,
            partition_offsets,
            partition_counts,
        } = self.value_buffers.consume();

        self.partition_assignment.update_offsets(&partition_offsets);

        let partition_offsets = self.partition_assignment.partition_offsets();

        Ok((values, partition_offsets, partition_counts))
    }

    async fn reset_state(&mut self) -> Result<(), IngestError> {
        self.delta_writer.reset();
        self.value_buffers.reset();
        self.delta_partition_offsets.clear();

        info!("State cleared.");

        let partitions: Vec<DataTypePartition> = self
            .partition_assignment
            .partition_offsets()
            .keys()
            .copied()
            .collect();

        // update offsets to the latest from the delta log
        for partition in partitions.iter() {
            let txn_app_id =
                delta_helpers::txn_app_id_for_partition(self.opts.app_id.as_str(), *partition);
            let version = self
                .delta_writer
                .last_transaction_version(txn_app_id.as_str());

            self.partition_assignment
                .assignment
                .insert(*partition, version);
            self.delta_partition_offsets.insert(*partition, version);
        }

        info!("Offsets from delta log applied to state.");

        info!("Seeking consumer");
        self.seek_consumer().await?;

        Ok(())
    }

    async fn seek_consumer(&self) -> Result<(), IngestError> {
        debug!(
            "Seek consumer invoked - current partition assignment: {:?}",
            self.partition_assignment.assignment
        );
        for (p, offset) in self.partition_assignment.assignment.iter() {
            match offset {
                Some(o) if *o == 0 => {
                    // TODO: Still sus of this no seek to 0 thing
                    debug!("Seeking consumer to beginning for partition {}. Delta log offset is 0, but seek to zero is not possible.", p);
                    self.consumer
                        .seek(&self.topic, *p, Offset::Beginning, Timeout::Never)?;
                }
                Some(o) => {
                    debug!(
                        "Seeking consumer to offset {} for partition {} found in delta log.",
                        o, p
                    );
                    self.consumer
                        .seek(&self.topic, *p, Offset::Offset(*o), Timeout::Never)?;
                }
                None => {
                    match &self.opts.starting_offsets {
                        StartingOffsets::Earliest => {
                            debug!("Seeking consumer to earliest offsets for partition {}", p);
                            self.consumer.seek(
                                &self.topic,
                                *p,
                                Offset::Beginning,
                                Timeout::Never,
                            )?;
                        }
                        StartingOffsets::Latest => {
                            debug!("Seeking consumer to latest offsets for partition {}", p);
                            self.consumer
                                .seek(&self.topic, *p, Offset::End, Timeout::Never)?;
                        }
                        StartingOffsets::Explicit(starting_offsets) => {
                            if let Some(offset) = starting_offsets.get(p) {
                                debug!("Seeking consumer to offset {} for partition {}. No offset is stored in delta log but explicit starting offsets are specified.", offset, p);
                                self.consumer.seek(
                                    &self.topic,
                                    *p,
                                    Offset::Offset(*offset),
                                    Timeout::Never,
                                )?;
                            } else {
                                debug!("Not seeking consumer. Offsets are explicit, but an entry is not provided for partition {}. `auto.offset.reset` from additional kafka settings will be used.", p);

                                // TODO: may need to lookup auto.offset.reset and force seek instead
                            }
                        }
                    };
                }
            };
        }
        Ok(())
    }
}

/// Provides a single interface into the multiple [`ValueBuffer`] instances used to handle each partition
#[derive(Debug)]
struct ValueBuffers {
    buffers: HashMap<DataTypePartition, ValueBuffer>,
    len: usize,
}

impl Default for ValueBuffers {
    fn default() -> Self {
        Self {
            buffers: HashMap::new(),
            len: 0,
        }
    }
}

impl ValueBuffers {
    fn add(&mut self, partition: DataTypePartition, offset: DataTypeOffset, value: Value) {
        let buffer = self
            .buffers
            .entry(partition)
            .or_insert_with(ValueBuffer::new);
        buffer.add(value, offset);
        self.len += 1;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn consume(&mut self) -> ConsumedBuffers {
        let mut partition_offsets = HashMap::new();
        let mut partition_counts = HashMap::new();

        let values = self
            .buffers
            .iter_mut()
            .filter_map(|(partition, buffer)| match buffer.consume() {
                Some((values, offset)) => {
                    partition_offsets.insert(*partition, offset);
                    partition_counts.insert(*partition, values.len());
                    Some(values)
                }
                None => None,
            })
            .flatten()
            .collect();

        self.len = 0;

        ConsumedBuffers {
            values,
            partition_offsets,
            partition_counts,
        }
    }

    pub fn reset(&mut self) {
        self.len = 0;
        self.buffers.clear();
    }
}

/// Buffer of values held in memory for a single Kafka partition.
#[derive(Debug)]
struct ValueBuffer {
    /// The offset of the last message stored in the buffer.
    last_offset: Option<DataTypeOffset>,
    /// The buffer of [`Value`] instances.
    values: Vec<Value>,
}

impl ValueBuffer {
    /// Creates a new [`ValueBuffer`] to store messages from a Kafka partition.
    fn new() -> Self {
        Self {
            last_offset: None,
            values: Vec::new(),
        }
    }

    /// Adds the value to buffer and stores its offset as the `last_offset` of the buffer.
    fn add(&mut self, value: Value, offset: DataTypeOffset) {
        self.last_offset = Some(offset);
        self.values.push(value);
    }

    /// Consumes and returns the buffer and last offset so it may be written to delta and clears internal state.
    fn consume(&mut self) -> Option<(Vec<Value>, DataTypeOffset)> {
        match self.last_offset {
            Some(last_offset) => {
                let consumed = (std::mem::take(&mut self.values), last_offset);
                self.last_offset = None;
                Some(consumed)
            }
            None => None,
        }
    }
}

struct ConsumedBuffers {
    values: Vec<Value>,
    partition_offsets: HashMap<DataTypePartition, DataTypeOffset>,
    partition_counts: HashMap<DataTypePartition, usize>,
}

/// Enum that represents a signal of an asynchronously received rebalance event that must be handled in the run loop.
/// Used to preserve correctness of messages stored in buffer after handling a rebalance event.
#[derive(Debug, PartialEq, Clone)]
pub enum RebalanceSignal {
    // None,
    PreRebalanceRevoke,
    PreRebalanceAssign(Vec<DataTypePartition>),
    PostRebalanceRevoke,
    PostRebalanceAssign(Vec<DataTypePartition>),
}

/// Contains the partition to offset assignment for a consumer.
struct PartitionAssignment {
    assignment: HashMap<DataTypePartition, Option<DataTypeOffset>>,
}

impl Default for PartitionAssignment {
    fn default() -> Self {
        Self {
            assignment: HashMap::new(),
        }
    }
}

impl PartitionAssignment {
    /// Resets the [`PartitionAssignment`] with the new list of partitions from Kafka.
    ///
    /// Note that this should be called only within loop on the executing thread.
    fn reset_with(&mut self, partitions: &[DataTypePartition]) {
        self.assignment.clear();
        for p in partitions {
            self.assignment.insert(*p, None);
        }
    }

    /// Updates the offsets stored in the [`PartitionAssignment`].
    fn update_offsets(&mut self, updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>) {
        for (k, v) in updated_offsets {
            if let Some(entry) = self.assignment.get_mut(k) {
                *entry = Some(*v);
            } else {
                warn!("Partition {} is not part of the assignment.", k);
            }
        }
    }

    /// Returns a copy of the current partition offsets.
    fn partition_offsets(&self) -> HashMap<DataTypePartition, DataTypeOffset> {
        let partition_offsets = self
            .assignment
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|o| (*k, *o)))
            .collect();

        partition_offsets
    }
}

/// Custom rdkafka [`Context`] associated with the rdkafka [`Consumer`].
/// Holds a sender to a channel of [`KafkaEvent`].
struct Context {
    sender: Sender<KafkaEvent>,
}

impl Context {
    fn new(sender: Sender<KafkaEvent>) -> Self {
        Self { sender }
    }
}

impl ClientContext for Context {}

impl ConsumerContext for Context {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Revoke => {
                debug!("PRE_REBALANCE - Revoke");
                let sender = self.sender.clone();

                tokio::spawn(async move {
                    let _ = sender
                        .send(KafkaEvent::Rebalance(RebalanceSignal::PreRebalanceRevoke))
                        .await;
                });
            }
            Rebalance::Assign(tpl) => {
                debug!("PRE_REBALANCE - Assign {:?}", tpl);
                let partitions = partition_vec_from_topic_partition_list(tpl);
                let sender = self.sender.clone();
                tokio::spawn(async move {
                    let _ = sender
                        .send(KafkaEvent::Rebalance(RebalanceSignal::PreRebalanceAssign(
                            partitions,
                        )))
                        .await;
                });
            }
            Rebalance::Error(e) => {
                panic!("PRE_REBALANCE - Unexpected Kafka error {:?}", e);
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Revoke => {
                debug!("POST_REBALANCE - Revoke");
                let sender = self.sender.clone();
                tokio::spawn(async move {
                    let _ = sender
                        .send(KafkaEvent::Rebalance(RebalanceSignal::PostRebalanceRevoke))
                        .await;
                });
            }
            Rebalance::Assign(tpl) => {
                debug!("POST_REBALANCE - Assign {:?}", tpl);
                let partitions = partition_vec_from_topic_partition_list(tpl);
                let sender = self.sender.clone();
                tokio::spawn(async move {
                    let _ = sender
                        .send(KafkaEvent::Rebalance(RebalanceSignal::PostRebalanceAssign(
                            partitions,
                        )))
                        .await;
                });
            }
            Rebalance::Error(e) => {
                panic!("POST_REBALANCE - Unexpected Kafka error {:?}", e);
            }
        }
    }
}

// Utility functions

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

/// Creates an [`rdkafka`] [`TopicPartitionList`] from a topic and vec of partitions.
fn topic_partition_list_from_partitions(
    topic: &str,
    partitions: &Vec<DataTypePartition>,
) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();

    tpl.add_topic_unassigned(topic);
    for p in partitions {
        tpl.add_partition(topic, *p);
    }

    tpl
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashmap;

    #[test]
    fn test_starting_offset_deserialization() {
        let earliest_offsets: StartingOffsets =
            StartingOffsets::from_string("earliest".to_string()).unwrap();
        assert_eq!(StartingOffsets::Earliest, earliest_offsets);

        let latest_offsets: StartingOffsets =
            StartingOffsets::from_string("latest".to_string()).unwrap();
        assert_eq!(StartingOffsets::Latest, latest_offsets);

        let explicit_offsets: StartingOffsets =
            StartingOffsets::from_string(r#"{"0":1,"1":2,"2":42}"#.to_string()).unwrap();
        assert_eq!(
            StartingOffsets::Explicit(hashmap! {
                0 => 1,
                1 => 2,
                2 => 42,
            }),
            explicit_offsets
        );

        let invalid = StartingOffsets::from_string(r#"{"not":"valid"}"#.to_string());

        match invalid {
            Err(StartingOffsetsParseError {
                string_to_parse, ..
            }) => {
                assert_eq!(r#"{"not":"valid"}"#.to_string(), string_to_parse);
            }
            _ => assert!(false, "StartingOffsets::from_string should return an Err"),
        }
    }
}
