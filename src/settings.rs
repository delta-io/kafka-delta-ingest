pub use crate::kafka::{AutoOffsetReset, DataTypeOffset, DataTypePartition};
use std::collections::HashMap;

/// Timeout for Kafka consumer poll events.
pub const DEFAULT_POLL_TIMEOUT: u64 = 10;

/// The default number of times to retry a delta commit when optimistic concurrency fails.
pub const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS: u32 = 10_000_000;

/// Number of seconds between sending lag metrics to statsd.
pub const DEFAULT_LAG_REPORT_SECONDS: u64 = 60;

/// The default input queue size for sending metrics to statsd.
pub const DEFAULT_INPUT_QUEUE_SIZE: usize = 100;

/// Name of the environment variable that defines the dynamodb lock partition key for dead letters.
pub const DEAD_LETTER_DYNAMO_LOCK_PARTITION_KEY_VALUE_VAR_NAME: &str =
    "DEAD_LETTER_DYNAMO_LOCK_PARTITION_KEY_VALUE";

/// The environment variable used to specify how many metrics should be written to the metrics queue before flushing to statsd.
pub const METRICS_INPUT_QUEUE_SIZE_VAR_NAME: &str = "KDI_METRICS_INPUT_QUEUE_SIZE";

/// The environment variable used to specify a prefix for metrics.
pub const METRICS_PREFIX_VAR_NAME: &str = "KDI_METRICS_PREFIX";

/// Type alias for a Vec of (partition, offset) tuples.
pub type SeekOffsets = Vec<(DataTypePartition, DataTypeOffset)>;

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
