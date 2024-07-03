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
use deltalake_core::operations::transaction::TableReference;
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::protocol::OutputMode;
use deltalake_core::{DeltaTable, DeltaTableError};
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
use serialization::{MessageDeserializer, MessageDeserializerFactory};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use url::Url;

mod coercions;
/// Doc
pub mod cursor;
mod dead_letters;
mod delta_helpers;
mod metrics;
mod offsets;
mod serialization;
mod transforms;
mod value_buffers;
/// Doc
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
use rdkafka::message::BorrowedMessage;
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
        source: Box<DataWriterError>,
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
        expected_version: i64,
        /// The version returned after the commit
        actual_version: i64,
    },
    /// Error returned if unable to construct a deserializer
    #[error("Unable to construct a message deserializer, source: {source}")]
    UnableToCreateDeserializer {
        /// The underlying error.
        source: anyhow::Error,
    },
}

/// Formats for message parsing
#[derive(Clone, Debug)]
pub enum MessageFormat {
    /// Parses messages as json and uses the inferred schema
    DefaultJson,

    /// Parses messages as json using the provided schema source. Will not use json schema files
    Json(SchemaSource),

    /// Parses avro messages using provided schema, schema registry or schema within file
    Avro(SchemaSource),
}

/// Source for schema
#[derive(Clone, Debug)]
pub enum SchemaSource {
    /// Use default behavior
    None,

    /// Use confluent schema registry url
    SchemaRegistry(Url),

    /// Use provided file for schema
    File(PathBuf),
}

#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
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
    /// Input format
    pub input_format: MessageFormat,
    /// Terminates when initial offsets are reached
    pub end_at_last_offsets: bool,
}

impl Default for IngestOptions {
    fn default() -> Self {
        IngestOptions {
            kafka_brokers: std::env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".into()),
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
            input_format: MessageFormat::DefaultJson,
            end_at_last_offsets: false,
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

    let _max_offsets = if opts.end_at_last_offsets {
        Some(fetch_latest_offsets(&topic, &consumer)?)
    } else {
        None
    };

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

    // Counter for how many messages have been consumed since startup.
    let mut consumed = 0u64;

    // The run loop
    loop {
        debug!("running the runloop");
        // Consume the next message from the stream.
        // Timeout if the next message is not received before the next flush interval.
        let duration = ingest_processor.consume_timeout_duration();
        let consume_result = tokio::time::timeout(duration, consumer.stream().next()).await;

        // Check for rebalance signal - skip the message if there is one.
        // After seek - Some worker will re-consume the message and see it again.
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

        // Initialize a flag which indicates whether the latency
        // timer has expired in this iteration of the run loop.
        let mut latency_timer_expired = false;

        // If the consume result includes a message, process it.
        // If latency timer expired instead - there's no message to process,
        // but we need to run flush checks.
        match consume_result {
            Ok(Some(message)) => {
                // Startup can take significant time,
                // so re-initialize the latency timer after consuming the first message.
                if consumed == 0 {
                    debug!("Latency timer reset ");
                    ingest_processor.latency_timer = Instant::now();
                }

                // Increment the consumed message counter.
                consumed += 1;

                let message = message?;

                if let Some(offset_map) = &_max_offsets {
                    if end_of_partition_reached(&message, offset_map) {
                        unassign_partition(cancellation_token.clone(), consumer.clone(), &message)?;
                    }
                }
                // Process the message if there wasn't a rebalance signal
                if let Err(e) = ingest_processor.process_message(message).await {
                    match e {
                        IngestError::AlreadyProcessedPartitionOffset { partition, offset } => {
                            debug!("Skipping message with partition {}, offset {} on topic {} because it was already processed", partition, offset, topic);
                            continue;
                        }
                        _ => return Err(e),
                    }
                }
            }
            Err(_) => {
                log::error!("Latency timer expired.");
                // Set the latency timer expired flag to indicate that
                // that the latency timer should be reset after flush checks.
                latency_timer_expired = true;
            }
            // Never seen this case actually happen.
            Ok(None) => {
                log::warn!("Message consumed is `None`");
                return Ok(());
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

        // If the latency timer expired on this iteration,
        // Reset it to now so we don't run flush checks again
        // until the next appropriate interval.
        if latency_timer_expired {
            debug!("latency timer expired, resetting");
            ingest_processor.latency_timer = Instant::now();
        }

        // Exit if the cancellation token is set.
        if cancellation_token.is_cancelled() {
            return Ok(());
        }
    }
}

fn end_of_partition_reached(
    message: &BorrowedMessage,
    offset_map: &HashMap<DataTypePartition, DataTypeOffset>,
) -> bool {
    let partition = message.partition() as DataTypePartition;
    let offset = &(message.offset() as DataTypeOffset);
    let max_offset = offset_map.get(&partition).unwrap();
    max_offset == offset
}

fn unassign_partition(
    cancellation_token: Arc<CancellationToken>,
    consumer: Arc<StreamConsumer<KafkaContext>>,
    message: &BorrowedMessage,
) -> Result<(), IngestError> {
    let mut tpl = TopicPartitionList::new();
    let partition = message.partition();
    tpl.add_partition(message.topic(), partition);

    // Remove the partition of this message from the assigned partitions
    let assignment = consumer
        .assignment()?
        .elements()
        .iter()
        .filter(|tp| {
            tp.topic() != message.topic()
                || (tp.topic() == message.topic() && tp.partition() != message.partition())
        })
        .map(|tp| ((tp.topic().to_string(), tp.partition()), tp.offset()))
        .collect::<HashMap<_, _>>();

    let new_assignment = TopicPartitionList::from_topic_map(&assignment)?;
    consumer.assign(&new_assignment)?;
    log::info!(
        "Reached the end of partition {}, removing assignment",
        partition
    );

    if new_assignment.count() == 0 {
        log::info!("Reached the end of partition {}, terminating", partition);
        cancellation_token.cancel();
    }

    Ok(())
}

fn fetch_latest_offsets(
    topic: &String,
    consumer: &Arc<StreamConsumer<KafkaContext>>,
) -> Result<HashMap<DataTypePartition, DataTypeOffset>, IngestError> {
    let metadata = &consumer.fetch_metadata(Some(topic.as_ref()), Timeout::Never)?;

    let partition_meta = metadata
        .topics()
        .iter()
        .filter(|t| t.name() == topic)
        .collect::<Vec<_>>()
        .first()
        .unwrap()
        .partitions();
    let partitions = partition_meta
        .iter()
        .map(|p| p.id() as DataTypePartition)
        .collect::<Vec<_>>();
    let result = get_high_watermark_map(topic.as_str(), consumer.clone(), partitions.into_iter())?;
    Ok(result)
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
            let rebalance_signal_val = rebalance_signal.write().await.take();
            match rebalance_signal_val {
                Some(RebalanceSignal::RebalanceAssign(partitions)) => {
                    info!(
                        "Handling rebalance assign. New assigned partitions are {:?}",
                        partitions
                    );
                    if let Err(err) = processor.table.update().await {
                        let mut rebalance_signal_val = rebalance_signal.write().await;
                        // Set the signal back,
                        // but only if it wasn't already replaced by a new one.
                        if rebalance_signal_val.is_none() {
                            *rebalance_signal_val =
                                Some(RebalanceSignal::RebalanceAssign(partitions));
                        }
                        Err(err.into())
                    } else {
                        partition_assignment.reset_with(partitions.as_slice());
                        processor.reset_state(partition_assignment)?;
                        Err(IngestError::RebalanceInterrupt)
                    }
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
    #[error("Kafka message deserialization failed")]
    AvroDeserialization { dead_letter: DeadLetter },
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
    message_deserializer: Box<dyn MessageDeserializer + Send>,
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
        let coercion_tree = coercions::create_coercion_tree(table.schema().unwrap());
        let delta_writer = DataWriter::for_table(&table, HashMap::new())?;
        let deserializer = match MessageDeserializerFactory::try_build(&opts.input_format) {
            Ok(deserializer) => deserializer,
            Err(e) => return Err(IngestError::UnableToCreateDeserializer { source: e }),
        };
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
            message_deserializer: deserializer,
        })
    }

    /// Returns the timeout duration to wait for the next message.
    fn consume_timeout_duration(&self) -> Duration {
        let elapsed_secs = self.latency_timer.elapsed().as_secs();

        let timeout_secs = if elapsed_secs >= self.opts.allowed_latency {
            0
        } else {
            self.opts.allowed_latency - elapsed_secs
        };

        Duration::from_secs(timeout_secs)
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
        match self.deserialize_message(&message).await {
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
            Err(
                MessageDeserializationError::JsonDeserialization { dead_letter }
                | MessageDeserializationError::AvroDeserialization { dead_letter },
            ) => {
                warn!(
                    "Deserialization failed - partition {}, offset {}, dead_letter {}",
                    partition,
                    offset,
                    dead_letter.error.as_ref().unwrap_or(&String::from("_")),
                );
                self.ingest_metrics.message_deserialization_failed();
                self.dlq.write_dead_letter(dead_letter).await?;
            }
        }

        Ok(())
    }

    /// Deserializes a message received from Kafka
    async fn deserialize_message<M>(
        &mut self,
        msg: &M,
    ) -> Result<Value, MessageDeserializationError>
    where
        M: Message + Send + Sync,
    {
        let message_bytes = match msg.payload() {
            Some(b) => b,
            None => return Err(MessageDeserializationError::EmptyPayload),
        };

        let value = self.message_deserializer.deserialize(message_bytes).await?;
        self.ingest_metrics
            .message_deserialized_size(message_bytes.len());
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

        if let Err(e) = self.delta_writer.write(values).await {
            if let DataWriterError::PartialParquetWrite {
                skipped_values,
                sample_error,
            } = *e
            {
                warn!(
                    "Partial parquet write, skipped {} values, sample ParquetError {:?}",
                    skipped_values.len(),
                    sample_error
                );

                let dead_letters = DeadLetter::vec_from_failed_parquet_rows(skipped_values);
                self.dlq.write_dead_letters(dead_letters).await?;
            } else {
                return Err(IngestError::DeltaWriteFailed {
                    ending_offsets: serde_json::to_string(&partition_offsets).unwrap(),
                    partition_counts: serde_json::to_string(&partition_counts).unwrap(),
                    source: *e,
                });
            }
        }

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
            .write_parquet_files(&self.table.table_uri())
            .await?;
        // Record file sizes
        for a in add.iter() {
            self.ingest_metrics.delta_file_size(a.size);
        }

        self.table.update().await?;
        if !self.are_partition_offsets_match() {
            return Err(IngestError::ConflictingOffsets);
        }

        if self.delta_writer.update_schema(&self.table)? {
            info!("Table schema has been updated");
            // Update the coercion tree to reflect the new schema
            let coercion_tree = coercions::create_coercion_tree(self.table.schema().unwrap());
            let _ = std::mem::replace(&mut self.coercion_tree, coercion_tree);

            return Err(IngestError::DeltaSchemaChanged);
        }

        // Try to commit
        let mut attempt_number: u32 = 0;
        let actions = build_actions(&partition_offsets, self.opts.app_id.as_str(), add);
        loop {
            let epoch_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as i64;
            let commit = deltalake_core::operations::transaction::CommitBuilder::default()
                .with_actions(actions.clone())
                .build(
                    self.table.state.as_ref().map(|s| s as &dyn TableReference),
                    self.table.log_store().clone(),
                    DeltaOperation::StreamingUpdate {
                        output_mode: OutputMode::Append,
                        query_id: self.opts.app_id.clone(),
                        epoch_id,
                    },
                )
                .await
                .map_err(DeltaTableError::from);
            match commit {
                Ok(v) => {
                    /*if v != version {
                        return Err(IngestError::UnexpectedVersionMismatch {
                            expected_version: version,
                            actual_version: v,
                        });
                    }
                    assert_eq!(v, version);*/
                    for (p, o) in &partition_offsets {
                        self.delta_partition_offsets.insert(*p, Some(*o));
                    }
                    if self.opts.write_checkpoints {
                        try_create_checkpoint(&mut self.table, v.version).await?;
                    }
                    record_write_lag(
                        self.topic.as_str(),
                        self.consumer.clone(),
                        &partition_offsets,
                        &self.ingest_metrics,
                    )?;
                    return Ok(v.version);
                }
                Err(e) => match e {
                    DeltaTableError::VersionAlreadyExists(_) => {
                        error!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing", DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS);
                        return Err(e.into());
                    }
                    // if store == "DeltaS3ObjectStore"
                    DeltaTableError::GenericError { source: _ } => {
                        error!("Delta write failed.. DeltaTableError: {}", e);
                        return Err(IngestError::InconsistentState(
                            "The remote dynamodb lock is non-acquirable!".to_string(),
                        ));
                    }
                    _ if attempt_number == 0 => return Err(e.into()),
                    _ => attempt_number += 1,
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
        debug!("latency_timer {:?}", self.latency_timer);
        debug!("elapsed_millis: {:?}", elapsed_millis);
        debug!("Value buffers has {} items", self.value_buffers.len());

        let should = self.value_buffers.len() > 0
            && (self.value_buffers.len() == self.opts.max_messages_per_batch)
            || (elapsed_millis >= (self.opts.allowed_latency * 1000) as u128);

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

        let should = self.delta_writer.buffer_len() > 0
            && (self.delta_writer.buffer_len() >= self.opts.min_bytes_per_file
                || elapsed_secs >= self.opts.allowed_latency);

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
                tokio::spawn(async move {
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

                tokio::spawn(async move {
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
    if let Ok(ca_pem) = std::env::var("KAFKA_DELTA_INGEST_CA") {
        kafka_client_config.set("ssl.ca.pem", ca_pem);
    }
    if let Ok(scram_json) = std::env::var("KAFKA_DELTA_INGEST_SCRAM_JSON") {
        let value: Value = serde_json::from_str(scram_json.as_str())
            .expect("KAFKA_DELTA_INGEST_SCRAM_JSON should be valid JSON");

        let username = value["username"]
            .as_str()
            .expect("'username' must be present in KAFKA_DELTA_INGEST_SCRAM_JSON");
        let password = value["password"]
            .as_str()
            .expect("'password' must be present in KAFKA_DELTA_INGEST_SCRAM_JSON");

        kafka_client_config.set("sasl.username", username);
        kafka_client_config.set("sasl.password", password);
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
) -> Result<Vec<DataTypeOffset>, KafkaError>
where
    I: Iterator<Item = DataTypePartition>,
{
    get_high_watermark_map(topic, consumer, partitions)
        .map(|hashmap| hashmap.into_values().collect::<Vec<_>>())
}

/// Fetches high watermarks (latest offsets) with partitions from Kafka from the iterator of partitions.
fn get_high_watermark_map<I>(
    topic: &str,
    consumer: Arc<StreamConsumer<KafkaContext>>,
    partitions: I,
) -> Result<HashMap<DataTypePartition, DataTypeOffset>, KafkaError>
where
    I: Iterator<Item = DataTypePartition>,
{
    partitions
        .map(|partition| {
            consumer
                .fetch_watermarks(topic, partition, Timeout::Never)
                .map(|(_, latest_offset)| (partition, latest_offset as DataTypeOffset))
        })
        .collect()
}
