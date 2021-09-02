use crate::transforms::Transformer;
use async_trait::async_trait;
use chrono::prelude::*;
use core::fmt::Debug;
use log::{error, info, warn};
use parquet::errors::ParquetError;
use rdkafka::message::BorrowedMessage;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::{deltalake_ext::*, transforms::TransformError};

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
    pub fn from_failed_deserialization(bytes: &[u8], err: serde_json::Error) -> Self {
        let timestamp = Utc::now();
        Self {
            base64_bytes: Some(base64::encode(bytes)),
            json_string: None,
            error: Some(err.to_string()),
            timestamp: timestamp.timestamp_nanos() / 1000,
        }
    }

    /// Creates a dead letter from a failed transform.
    /// `base64_bytes` will always be `None`.
    pub fn from_failed_transform(value: &Value, err: TransformError) -> Self {
        let timestamp = Utc::now();
        Self {
            base64_bytes: None,
            json_string: Some(value.to_string()),
            error: Some(err.to_string()),
            timestamp: timestamp.timestamp_nanos() / 1000,
        }
    }

    /// Creates a dead letter from a record that fails on parquet write.
    /// `base64_bytes` will always be `None`.
    /// `json_string` will contain the stringified JSON that was not writeable to parquet.
    pub fn from_failed_parquet_row(value: &Value, err: ParquetError) -> Self {
        let timestamp = Utc::now();
        Self {
            base64_bytes: None,
            json_string: Some(value.to_string()),
            error: Some(err.to_string()),
            timestamp: timestamp.timestamp_nanos() / 1000,
        }
    }

    /// Creates a vector of tuples where the first element is the
    /// stringified JSON value that was not writeable to parquet and
    /// the second element is the `ParquetError` that occurred for that record when attempting the write.
    pub fn vec_from_failed_parquet_rows(failed: Vec<(Value, ParquetError)>) -> Vec<Self> {
        failed
            .iter()
            .map(|(v, e)| Self::from_failed_parquet_row(v, e.to_owned()))
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
    #[error("Delta write failed: {source}")]
    DeltaWriter {
        #[from]
        source: DeltaWriterError,
    },

    /// Error returned by the internal dead letter transformer.
    #[error("TransformError: {source}")]
    Transform {
        #[from]
        source: TransformError,
    },

    /// Error returned when the DeltaSinkDeadLetterQueue is used but no table uri is specified.
    #[error("No table_uri for DeltaSinkDeadLetterQueue")]
    NoTableUri,
}

/// Options that should be passed to `dlq_from_opts` to create the desired [DeadLetterQueue] instance.
pub struct DeadLetterQueueOptions {
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
pub trait DeadLetterQueue: Send + Sync {
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
pub async fn dlq_from_opts(
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
pub struct NoopDeadLetterQueue {}

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
pub struct LoggingDeadLetterQueue {}

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
/// and be compatible with the [DeadLetter] struct.
pub struct DeltaSinkDeadLetterQueue {
    delta_writer: DeltaWriter,
    transformer: Transformer,
    write_checkpoints: bool,
}

impl DeltaSinkDeadLetterQueue {
    pub async fn from_options(
        options: DeadLetterQueueOptions,
    ) -> Result<Self, DeadLetterQueueError> {
        match &options.delta_table_uri {
            Some(table_uri) => Ok(Self {
                delta_writer: DeltaWriter::for_table_path(table_uri).await?,
                transformer: Transformer::from_transforms(&options.dead_letter_transforms)?,
                write_checkpoints: options.write_checkpoints,
            }),
            _ => Err(DeadLetterQueueError::NoTableUri),
        }
    }
}

#[async_trait]
impl DeadLetterQueue for DeltaSinkDeadLetterQueue {
    /// Writes dead letters to the delta table specified in [DeadLetterQueueOptions].
    /// Transforms specified in [DeadLetterQueueOptions] are applied before write.
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

        info!("Starting insert_all");
        let version = self.delta_writer.insert_all(values).await?;

        if self.write_checkpoints {
            self.delta_writer.try_create_checkpoint(version).await?;
        }

        info!("Completed insert_all");

        Ok(())
    }
}
