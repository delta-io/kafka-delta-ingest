use arrow::{datatypes::SchemaRef, error::ArrowError};
use deltalake::{
    action::Stats, checkpoints::CheckpointError, DeltaDataTypeVersion, DeltaTableError,
    StorageError, UriError,
};
use jmespatch::JmespathError;
use parquet::errors::ParquetError;
use rdkafka::error::KafkaError;
use serde_json::Value;
use std::sync::Arc;

use crate::{
    dead_letters::DeadLetter,
    kafka::{DataTypeOffset, DataTypePartition},
};

/// Unhandled error that will terminate an ingest process.
#[derive(thiserror::Error, Debug)]
pub enum IngestError {
    #[error("Kafka error: {source}")]
    Kafka {
        #[from]
        source: KafkaError,
    },

    #[error("DeltaTable error: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("Writer error: {source}")]
    Writer {
        #[from]
        source: DataWriterError,
    },

    #[error("DeadLetterQueue error: {source}")]
    DeadLetterQueueError {
        #[from]
        source: DeadLetterQueueError,
    },

    #[error("TransformError: {source}")]
    Transform {
        #[from]
        source: TransformError,
    },

    #[error("IngestMetrics failed {source}")]
    IngestMetrics {
        #[from]
        source: IngestMetricsError,
    },

    #[error("Processing failed {source}")]
    Processing {
        #[from]
        source: ProcessingError,
    },
}

/// Error returned by [`Processor`]
#[derive(thiserror::Error, Debug)]
pub enum ProcessingError {
    #[error("Timer error {source}")]
    Timer {
        #[from]
        source: TimerError,
    },

    #[error("WriteOffsets error: {source}")]
    WriteOffsets {
        #[from]
        source: WriteOffsetsError,
    },

    #[error("DeadLetterQueue error: {source}")]
    DeadLetterQueueError {
        #[from]
        source: DeadLetterQueueError,
    },

    #[error("Writer error: {source}")]
    DataWriter {
        #[from]
        source: DataWriterError,
    },

    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("Kafka error: {source}")]
    Kafka {
        #[from]
        source: KafkaError,
    },
}

/// Error returned by [`Timer`]
#[derive(thiserror::Error, Debug)]
pub enum TimerError {
    #[error("Timer expiration checked before initialization")]
    Uninitialized,
}

/// Error returned when message deserialization fails.
#[derive(thiserror::Error, Debug)]
pub enum MessageDeserializationError {
    #[error("Kafka message contained empty payload")]
    EmptyPayload,

    #[error("Kafka message deserialization failed")]
    JsonDeserialization { dead_letter: DeadLetter },
}

/// Error returned by [`Transformer`].
#[derive(thiserror::Error, Debug)]
pub enum TransformError {
    #[error("Unable to mutate non-object value {value}")]
    ValueNotAnObject { value: Value },

    #[error("JmespathError: {source}")]
    JmesPath {
        #[from]
        source: JmespathError,
    },

    #[error("serde_json::Error: {source}")]
    Json {
        #[from]
        source: serde_json::Error,
    },
}

/// Error returned when message processing fails.
#[derive(thiserror::Error, Debug)]
pub enum MessageProcessingError {
    #[error("Kafka message contained empty payload")]
    AlreadyProcessedOffset {
        partition: DataTypePartition,
        offset: DataTypeOffset,
    },
    #[error("Kafka message contained empty payload")]
    Deserialization {
        #[from]
        source: MessageDeserializationError,
    },
    #[error("Kafka message contained empty payload")]
    Transform { value: Value, error: TransformError },
}

/// Error returned by [`IngestMetrics`].
#[derive(thiserror::Error, Debug)]
pub enum IngestMetricsError {
    #[error("Could not parse {0} provided in METRICS_INPUT_QUEUE_SIZE env variable")]
    InvalidMetricsInputQueueSize(String),
}

/// Error returned when writing offsets.
#[derive(thiserror::Error, Debug)]
pub enum WriteOffsetsError {
    #[error("Stored offsets are lower than provided: {0}")]
    InconsistentStoredOffsets(String),

    #[error("DeltaTable error: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },
}

/// Error returned when writing data to arrow or parquet fails.
#[derive(thiserror::Error, Debug)]
pub enum DataWriterError {
    #[error("Arrow RecordBatch created from JSON buffer is a None value")]
    EmptyRecordBatch,

    #[error("Delta transaction log contains conflicting offsets for assigned partitions.")]
    ConflictingOffsets,

    #[error("Delta schema changed and must be updated.")]
    DeltaSchemaChanged,

    #[error("Failed to write some values to parquet. Sample error: {sample_error}.")]
    PartialParquetWrite {
        skipped_values: Vec<(Value, ParquetError)>,
        sample_error: ParquetError,
    },

    #[error("Serialization of delta log statistics failed")]
    StatsSerializationFailed { stats: Stats },

    #[error("Missing partition column: {0}")]
    MissingPartitionColumn(String),

    #[error("Arrow RecordBatch schema does not match: RecordBatch schema: {record_batch_schema}, {expected_schema}")]
    SchemaMismatch {
        record_batch_schema: SchemaRef,
        expected_schema: Arc<arrow::datatypes::Schema>,
    },

    #[error("Committed delta version {actual_version} does not match the version specified in the commit attempt {expected_version}")]
    UnexpectedVersionMismatch {
        expected_version: DeltaDataTypeVersion,
        actual_version: DeltaDataTypeVersion,
    },

    #[error("Checkpoint error: {source}")]
    Checkpoint {
        #[from]
        source: CheckpointError,
    },

    #[error("Delta table is in an inconsistent state: {0}")]
    InconsistentState(String),

    #[error("Record {0} is not a JSON object")]
    InvalidRecord(String),

    #[error("Invalid table path: {}", .source)]
    UriError {
        #[from]
        source: UriError,
    },

    #[error("Storage interaction failed: {source}")]
    Storage {
        #[from]
        source: StorageError,
    },

    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("Arrow interaction failed: {source}")]
    Arrow {
        #[from]
        source: ArrowError,
    },

    #[error("Parquet write failed: {source}")]
    Parquet {
        #[from]
        source: ParquetError,
    },

    #[error("std::io::Error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
}

/// Error returned when a dead letter write fails.
#[derive(thiserror::Error, Debug)]
pub enum DeadLetterQueueError {
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        #[from]
        source: serde_json::Error,
    },

    #[error("Write failed: {source}")]
    Writer {
        #[from]
        source: DataWriterError,
    },

    #[error("TransformError: {source}")]
    Transform {
        #[from]
        source: TransformError,
    },

    #[error("CheckpointErrorError error: {source}")]
    Checkpoint {
        #[from]
        source: CheckpointError,
    },

    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("No table_uri for DeltaSinkDeadLetterQueue")]
    NoTableUri,
}
