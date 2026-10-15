use crate::transforms::Transformer;
use async_trait::async_trait;
use chrono::prelude::*;
use core::fmt::Debug;
use deltalake_core::parquet::errors::ParquetError;
use deltalake_core::{DeltaTable, DeltaTableError};
use log::{info, warn};
use rdkafka::message::BorrowedMessage;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::{transforms::TransformError, writer::*};

/// Struct that represents a dead letter record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeadLetter {
    /// Base64 encoded bytes captured when message deserialization from Kafka fails.
    pub base64_bytes: Option<String>,
    /// JSON string captured when either transform or parquet write fails.
    pub json_string: Option<String>,
    /// Error string that correlates with the failure.
    /// The error type will vary depending on the context.
    pub error: Option<String>,
    /// Timestamp microseconds representing when the dead letter was created.
    /// Microseconds are used for compatability with the deltalake `timestamp` type.
    pub timestamp: i64,
}

impl DeadLetter {
    /// Creates a dead letter from bytes that failed deserialization.
    /// `json_string` will always be `None`.
    pub(crate) fn from_failed_deserialization(bytes: &[u8], err: String) -> Self {
        let timestamp = Utc::now();
        Self {
            base64_bytes: Some(base64::encode(bytes)),
            json_string: None,
            error: Some(err),
            timestamp: timestamp
                .timestamp_nanos_opt()
                .expect("Failed to convert timezone to nanoseconds")
                / 1000,
        }
    }

    /// Creates a dead letter from a failed transform.
    /// `base64_bytes` will always be `None`.
    pub(crate) fn from_failed_transform(value: &Value, err: TransformError) -> Self {
        let timestamp = Utc::now();
        Self {
            base64_bytes: None,
            json_string: Some(value.to_string()),
            error: Some(err.to_string()),
            timestamp: timestamp
                .timestamp_nanos_opt()
                .expect("Failed to convert timezone to nanoseconds")
                / 1000,
        }
    }

    /// Creates a dead letter from a record that fails on parquet write.
    /// `base64_bytes` will always be `None`.
    /// `json_string` will contain the stringified JSON that was not writeable to parquet.
    pub(crate) fn from_failed_parquet_row(value: &Value, err: &ParquetError) -> Self {
        let timestamp = Utc::now();
        Self {
            base64_bytes: None,
            json_string: Some(value.to_string()),
            error: Some(err.to_string()),
            timestamp: timestamp
                .timestamp_nanos_opt()
                .expect("Failed to convert timezone to nanoseconds")
                / 1000,
        }
    }

    /// Creates a vector of tuples where the first element is the
    /// stringified JSON value that was not writeable to parquet and
    /// the second element is the `ParquetError` that occurred for that record when attempting the write.
    pub(crate) fn vec_from_failed_parquet_rows(failed: Vec<(Value, ParquetError)>) -> Vec<Self> {
        failed
            .iter()
            .map(|(v, e)| Self::from_failed_parquet_row(v, e))
            .collect()
    }
}

/// Error returned when a dead letter write fails.
#[derive(thiserror::Error, Debug)]
pub enum DeadLetterQueueError {
    /// Error returned when JSON serialization of a [DeadLetter] fails.
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        #[from]
        source: serde_json::Error,
    },

    /// Error returned when a write to the dead letter delta table used by [DeltaSinkDeadLetterQueue] fails.
    #[error("Write failed: {source}")]
    Writer {
        #[from]
        source: Box<DataWriterError>,
    },

    /// Error returned by the internal dead letter transformer.
    #[error("TransformError: {source}")]
    Transform {
        #[from]
        source: TransformError,
    },

    /// DeltaTable returned an error.
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// The wrapped [`DeltaTableError`]
        #[from]
        source: DeltaTableError,
    },

    /// Error returned when the DeltaSinkDeadLetterQueue is used but no table uri is specified.
    #[error("No table_uri for DeltaSinkDeadLetterQueue")]
    NoTableUri,
}

/// Options that should be passed to `dlq_from_opts` to create the desired [DeadLetterQueue] instance.
pub(crate) struct DeadLetterQueueOptions {
    /// Table URI of the delta table to write dead letters to. Implies usage of the DeltaSinkDeadLetterQueue.
    pub delta_table_uri: Option<String>,
    /// A list of transforms to apply to dead letters before writing to delta.
    pub dead_letter_transforms: HashMap<String, String>,
    /// Whether to write checkpoints on every 10th version of the dead letter table.
    pub write_checkpoints: bool,
}

/// Trait that defines a dead letter queue interface.
/// Available implementations are:
/// * [NoopDeadLetterQueue] (the default)
/// * [DeltaSinkDeadLetterQueue]
/// * [LoggingDeadLetterQueue]
///
/// The [LoggingDeadLetterQueue] is intended for local development only
/// and is not provided by the [dlq_from_opts] factory method.
#[async_trait]
pub(crate) trait DeadLetterQueue: Send {
    /// Writes one [DeadLetter] to the [DeadLetterQueue].
    async fn write_dead_letter(
        &mut self,
        dead_letter: DeadLetter,
    ) -> Result<(), DeadLetterQueueError> {
        self.write_dead_letters(vec![dead_letter]).await
    }

    /// Writes a vector of [DeadLetter]s to the [DeadLetterQueue]
    async fn write_dead_letters(
        &mut self,
        dead_letters: Vec<DeadLetter>,
    ) -> Result<(), DeadLetterQueueError>;
}

/// Factory method for creating a [DeadLetterQueue] based on the passed options.
/// The default implementation is [NoopDeadLetterQueue].
/// To opt-in for the [DeltaSinkDeadLetterQueue], the `delta_table_uri` should be set in options.
pub(crate) async fn dlq_from_opts(
    options: DeadLetterQueueOptions,
) -> Result<Box<dyn DeadLetterQueue>, DeadLetterQueueError> {
    if options.delta_table_uri.is_some() {
        Ok(Box::new(
            DeltaSinkDeadLetterQueue::from_options(options).await?,
        ))
    } else {
        Ok(Box::new(NoopDeadLetterQueue {}))
    }
}

/// Default implementation of [DeadLetterQueue] which does nothing.
/// This is used as the default to avoid forcing users to setup additional infrastructure for capturing dead letters.
/// and avoid any risk of exposing PII in logs,
pub(crate) struct NoopDeadLetterQueue {}

#[async_trait]
impl DeadLetterQueue for NoopDeadLetterQueue {
    async fn write_dead_letters(
        &mut self,
        _dead_letters: Vec<DeadLetter>,
    ) -> Result<(), DeadLetterQueueError> {
        // noop
        Ok(())
    }
}

/// Implementation of the [DeadLetterQueue] trait that writes dead letter content as warn logs.
/// This implementation is currently only intended for debug development usage.
/// Be mindful of your PII when using this implementation.
#[allow(dead_code)]
pub(crate) struct LoggingDeadLetterQueue {}

#[async_trait]
impl DeadLetterQueue for LoggingDeadLetterQueue {
    async fn write_dead_letters(
        &mut self,
        dead_letters: Vec<DeadLetter>,
    ) -> Result<(), DeadLetterQueueError> {
        for dead_letter in dead_letters {
            warn!("DeadLetter: {:?}", dead_letter);
        }

        Ok(())
    }
}

/// Implementation of the [DeadLetterQueue] trait that writes dead letters to a delta table.
/// NOTE: The delta table where dead letters are written must be created beforehand
/// and be based on the [DeadLetter] struct.
/// DeadLetter transforms may be specified to enrich the serialized [DeadLetter] before writing it to the table.
///
/// For example, given a delta table schema created from:
///
/// ```sql
/// CREATE TABLE `kafka_delta_ingest`.`dead_letters` (
///   `base64_bytes` STRING COMMENT 'Base 64 encoded bytes of a message that failed deserialization.',
///   `json_string` STRING COMMENT 'JSON string captured when either transform or parquet write fails for a message.',
///   `error` STRING COMMENT 'Error string captured when the dead letter failed.',
///   `timestamp` TIMESTAMP COMMENT 'Timestamp when the dead letter was created.',
///   `date` STRING COMMENT '(e.g. 2021-01-01) Date when the dead letter was created.')
/// USING DELTA
/// PARTITIONED BY (date)
/// ```
///
/// A dead letter transform with key: `date` and value: `substr(epoch_micros_to_iso8601(timestamp),`0`,`10`)` should be provided to generate the `date` field.
pub(crate) struct DeltaSinkDeadLetterQueue {
    table: DeltaTable,
    delta_writer: DataWriter,
    transformer: Transformer,
    write_checkpoints: bool,
}

impl DeltaSinkDeadLetterQueue {
    pub(crate) async fn from_options(
        options: DeadLetterQueueOptions,
    ) -> Result<Self, DeadLetterQueueError> {
        match &options.delta_table_uri {
            Some(table_uri) => {
                let table = crate::delta_helpers::load_table(table_uri, HashMap::new()).await?;
                let delta_writer = DataWriter::for_table(&table, HashMap::new())?;

                Ok(Self {
                    table,
                    delta_writer,
                    transformer: Transformer::from_transforms(&options.dead_letter_transforms)?,
                    write_checkpoints: options.write_checkpoints,
                })
            }
            _ => Err(DeadLetterQueueError::NoTableUri),
        }
    }
}

#[async_trait]
impl DeadLetterQueue for DeltaSinkDeadLetterQueue {
    /// Writes dead letters to the delta table specified in [DeadLetterQueueOptions].
    /// Transforms specified in [DeadLetterQueueOptions] are applied before write.
    /// If the `write_checkpoints` options is specified - writes a checkpoint on every version divisible by 10.
    async fn write_dead_letters(
        &mut self,
        dead_letters: Vec<DeadLetter>,
    ) -> Result<(), DeadLetterQueueError> {
        let values: Result<Vec<Value>, _> = dead_letters
            .iter()
            .map(|dl| {
                serde_json::to_value(dl)
                    .map_err(|e| DeadLetterQueueError::SerdeJson { source: e })
                    .and_then(|mut v| {
                        self.transformer
                            .transform(&mut v, None as Option<&BorrowedMessage>)?;
                        Ok(v)
                    })
            })
            .collect();
        let values = values?;

        let version = self
            .delta_writer
            .insert_all(&mut self.table, values)
            .await?;

        if self.write_checkpoints {
            crate::delta_helpers::try_create_checkpoint(&mut self.table, version).await?;
        }

        info!(
            "Inserted {} dead letters to {}",
            dead_letters.len(),
            self.table.table_uri(),
        );

        Ok(())
    }
}
